//! A small backtracking regular-expression engine, for the ported tooling.
//!
//! The perf tooling is being ported from Python to Rust one tool at a time, and
//! several of the ported checks are defined as patterns over the markup the
//! renderer wrote: a check that reads a bound label's box back out of the SVG,
//! or the numbers a stated reading carries, is only the same check as its Python
//! twin if it is the *same pattern*. Hand-coding thirty-odd scanners would make
//! each one a second, quietly different, authority for the same rule.
//!
//! So the patterns stay patterns. This engine implements the subset of Python's
//! `re` the ported tools use -- literals, `.`, character classes with ranges and
//! negation, the `\d`/`\w`/`\s` classes, the `^`/`$`/`\b` assertions, capturing
//! and non-capturing groups, named groups, alternation, and greedy and lazy
//! quantifiers -- and nothing else. It is deliberately not a general engine: no
//! lookaround, no backreferences, no flag beyond `re.S` (dot matches a newline),
//! because none is used and a feature nobody needs is a feature nobody has
//! verified.
//!
//! Matching is recursive backtracking over `char`s, and positions are character
//! offsets rather than bytes, so a caller slicing a match out of the text it
//! matched gets the same substring Python would.
//!
//! Failure restores the captures: every attempt that sets a group and then fails
//! puts the group back, so `None` from [`Regex::search`] leaves the caller's
//! state exactly as it found it.
//!
//! Three of Python's flags are honoured, because a ported checker's patterns use
//! them: `re.S` ([`Flags::dotall`]), `re.M` ([`Flags::multiline`], where `^` and
//! `$` also match at a line boundary) and `re.I` ([`Flags::ignorecase`], folded
//! over ASCII -- the documents these patterns read are ASCII, and folding over
//! ASCII keeps every match offset pointing into the original text). `\A` and
//! `\Z` stay string anchors under `re.M`, as Python's do.

use std::collections::BTreeMap;

/// The `re` flags a ported pattern may set.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Flags {
    /// `re.S`: `.` matches a newline.
    pub dotall: bool,
    /// `re.M`: `^`/`$` also match at a line boundary.
    pub multiline: bool,
    /// `re.I`: matching folds ASCII case.
    pub ignorecase: bool,
}

/// The `\w` class, in Python's `re` sense over the ASCII this tooling reads.
fn is_word(character: char) -> bool {
    character.is_ascii_alphanumeric() || character == '_'
}

fn is_space(character: char) -> bool {
    character.is_ascii_whitespace()
}

fn is_digit(character: char) -> bool {
    character.is_ascii_digit()
}

#[derive(Debug, Clone)]
enum ClassItem {
    Range(char, char),
    /// `.` under `re.S`: every character, newline included.
    DotAny,
    Digit,
    NotDigit,
    Word,
    NotWord,
    Space,
    NotSpace,
}

#[derive(Debug, Clone)]
enum Node {
    Char(char),
    Any,
    Class {
        negate: bool,
        items: Vec<ClassItem>,
    },
    /// `^`: the text's start always, and under `re.M` also a line's start.
    Start(bool),
    /// `$`: the text's end always, and under `re.M` also a line's end.
    End(bool),
    WordBoundary,
    NotWordBoundary,
    Group {
        index: Option<usize>,
        inner: Box<Node>,
    },
    /// A zero-width assertion: the inner pattern must (or must not) match at
    /// this position, and consumes nothing.
    Look {
        inner: Box<Node>,
        positive: bool,
    },
    /// `\A`: start of the whole text, never a line boundary.
    TextStart,
    /// `\Z`: end of the whole text, never a line boundary.
    TextEnd,
    Concat(Vec<Node>),
    Alt(Vec<Node>),
    Repeat {
        inner: Box<Node>,
        min: usize,
        max: usize,
        greedy: bool,
    },
}

const UNBOUNDED: usize = usize::MAX;

/// One matched span, with its capture groups as text.
#[derive(Debug, Clone)]
pub struct Match {
    pub start: usize,
    pub end: usize,
    whole: String,
    groups: Vec<Option<String>>,
    spans: Vec<Option<(usize, usize)>>,
    names: BTreeMap<String, usize>,
}

impl Match {
    /// The whole match's text.
    pub fn whole(&self) -> String {
        self.whole.clone()
    }

    /// Group `index` (`0` is the whole match), or `None` when it did not take
    /// part in the match.
    pub fn group(&self, index: usize) -> Option<String> {
        if index == 0 {
            return Some(self.whole.clone());
        }
        self.groups.get(index - 1).cloned().flatten()
    }

    /// A named group's text, or `None`.
    pub fn named(&self, name: &str) -> Option<String> {
        let index = *self.names.get(name)?;
        self.group(index)
    }

    /// Group `index`'s `(start, end)` character offsets, or `None` when it did
    /// not take part in the match.
    pub fn group_span(&self, index: usize) -> Option<(usize, usize)> {
        if index == 0 {
            return Some((self.start, self.end));
        }
        self.spans.get(index - 1).copied().flatten()
    }
}

/// A compiled pattern.
#[derive(Debug, Clone)]
pub struct Regex {
    root: Node,
    names: BTreeMap<String, usize>,
    group_count: usize,
    ignorecase: bool,
}

/// A pattern that could not be compiled. The ported tools' patterns are
/// literals, so this is a programming error rather than an input error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegexError(pub String);

impl std::fmt::Display for RegexError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "invalid regex: {}", self.0)
    }
}

struct Parser {
    chars: Vec<char>,
    pos: usize,
    group_count: usize,
    names: BTreeMap<String, usize>,
    ignorecase: bool,
    multiline: bool,
}

impl Regex {
    /// Compile a pattern. `dotall` is Python's `re.S`.
    pub fn new(pattern: &str, dotall: bool) -> Result<Regex, RegexError> {
        Regex::new_with_flags(
            pattern,
            Flags {
                dotall,
                ..Flags::default()
            },
        )
    }

    /// Compile a pattern with Python's `re` flags.
    pub fn new_with_flags(pattern: &str, flags: Flags) -> Result<Regex, RegexError> {
        let mut parser = Parser {
            chars: pattern.chars().collect(),
            pos: 0,
            group_count: 0,
            names: BTreeMap::new(),
            ignorecase: flags.ignorecase,
            multiline: flags.multiline,
        };
        let root = parser.alternation()?;
        if parser.pos != parser.chars.len() {
            return Err(RegexError(format!(
                "unexpected {:?} at pattern offset {}",
                parser.chars[parser.pos], parser.pos
            )));
        }
        // `dotall` is folded into the tree by rewriting `.` into a negated empty
        // class, so the matcher's hot path carries no flag lookup.
        let root = if flags.dotall {
            rewrite_dotall(root)
        } else {
            root
        };
        Ok(Regex {
            root,
            names: parser.names,
            group_count: parser.group_count,
            ignorecase: flags.ignorecase,
        })
    }

    /// The caller's text, and the text the matcher sees: ASCII-folded when
    /// `re.I` is set. Both are the same length, so a match's offsets index the
    /// original -- which is what a captured group must be read from, since
    /// Python returns the document's own spelling rather than the folded one.
    fn texts(&self, text: &str) -> (Vec<char>, Vec<char>) {
        let original: Vec<char> = text.chars().collect();
        if self.ignorecase {
            let folded = original.iter().map(|c| c.to_ascii_lowercase()).collect();
            (original, folded)
        } else {
            (original.clone(), original)
        }
    }

    fn empty_captures(&self) -> Captures {
        // Slot 0 is the whole match, which Python numbers as group 0 and never
        // reports through `groups`; keeping it makes the group numbering the
        // pattern's own rather than an off-by-one the matcher has to remember.
        vec![None; self.group_count + 1]
    }

    /// The first match at or after `from` (a character offset), or `None`.
    fn search_from(&self, text: &[char], from: usize) -> Option<(usize, usize, Captures)> {
        for start in from..=text.len() {
            let mut captures = self.empty_captures();
            let mut found: Option<usize> = None;
            let matched = run(&self.root, text, start, &mut captures, &mut |_caps, end| {
                found = Some(end);
                true
            });
            if matched {
                return Some((
                    start,
                    found.expect("a successful match records its end"),
                    captures,
                ));
            }
        }
        None
    }

    fn materialize(&self, chars: &[char], found: (usize, usize, Captures)) -> Match {
        let (start, end, captures) = found;
        let groups = captures
            .iter()
            .skip(1)
            .map(|entry| entry.map(|(a, b)| chars[a..b].iter().collect::<String>()))
            .collect();
        Match {
            start,
            end,
            whole: chars[start..end].iter().collect(),
            groups,
            spans: captures.iter().skip(1).copied().collect(),
            names: self.names.clone(),
        }
    }

    /// The first match's `(start, end)` at or after `from`, in character
    /// offsets, for a caller that walks a document itself.
    pub fn search_span_in(&self, text: &[char], from: usize) -> Option<(usize, usize)> {
        let found = self.search_from(text, from)?;
        Some((found.0, found.1))
    }

    /// The match's `(start, end)` when it begins at the document's start.
    pub fn match_span(&self, text: &str) -> Option<(usize, usize)> {
        let found = self.match_at(text)?;
        Some((found.start, found.end))
    }

    /// Python's `re.match(text, pos)`: whether the pattern matches starting at
    /// `pos` (a character offset), free at the end.
    pub fn match_from(&self, text: &str, from: usize) -> bool {
        let (_original, chars) = self.texts(text);
        let mut captures = self.empty_captures();
        run(
            &self.root,
            &chars,
            from,
            &mut captures,
            &mut |_caps, _stop| true,
        )
    }

    /// The first match, or `None`.
    pub fn search(&self, text: &str) -> Option<Match> {
        let (original, chars) = self.texts(text);
        let found = self.search_from(&chars, 0)?;
        Some(self.materialize(&original, found))
    }

    /// Whether the pattern matches anywhere.
    pub fn is_match(&self, text: &str) -> bool {
        let (_original, chars) = self.texts(text);
        self.search_from(&chars, 0).is_some()
    }

    /// Python's `re.match`: anchored at the start, free at the end.
    pub fn match_at(&self, text: &str) -> Option<Match> {
        let (original, chars) = self.texts(text);
        let mut captures = self.empty_captures();
        let mut end: Option<usize> = None;
        let matched = run(&self.root, &chars, 0, &mut captures, &mut |_caps, stop| {
            end = Some(stop);
            true
        });
        if !matched {
            return None;
        }
        Some(self.materialize(&original, (0, end.expect("matched"), captures)))
    }

    /// Python's `re.fullmatch`: the whole string, anchors not required.
    pub fn full_match(&self, text: &str) -> Option<Match> {
        let (original, chars) = self.texts(text);
        let mut captures = self.empty_captures();
        let mut end: Option<usize> = None;
        let matched = run(&self.root, &chars, 0, &mut captures, &mut |_caps, stop| {
            if stop == chars.len() {
                end = Some(stop);
                true
            } else {
                false
            }
        });
        if !matched {
            return None;
        }
        Some(self.materialize(&original, (0, end.expect("matched"), captures)))
    }

    /// Every non-overlapping match, in order.
    pub fn find_iter(&self, text: &str) -> Vec<Match> {
        let (original, chars) = self.texts(text);
        let mut out = Vec::new();
        let mut position = 0;
        while position <= chars.len() {
            let Some(found) = self.search_from(&chars, position) else {
                break;
            };
            let end = found.1;
            let start = found.0;
            // An empty match cannot advance the scan by itself, or the loop
            // would never terminate.
            position = if end == start { end + 1 } else { end };
            out.push(self.materialize(&original, found));
        }
        out
    }

    /// Python's `re.findall`: every match's group texts in group order, or each
    /// whole match when the pattern has no groups.
    pub fn find_all(&self, text: &str) -> Vec<Vec<Option<String>>> {
        self.find_iter(text)
            .iter()
            .map(|found| {
                if self.group_count == 0 {
                    vec![Some(found.whole())]
                } else {
                    (1..=self.group_count)
                        .map(|index| found.group(index))
                        .collect()
                }
            })
            .collect()
    }

    /// Python's `re.findall` for a zero-group pattern: the whole matches.
    pub fn find_all_whole(&self, text: &str) -> Vec<String> {
        self.find_iter(text).iter().map(Match::whole).collect()
    }

    /// Python's `re.sub(pattern, replacement, text)` with a literal
    /// replacement: every match replaced.
    pub fn replace_all(&self, text: &str, replacement: &str) -> String {
        let (chars, _folded) = self.texts(text);
        let mut out = String::new();
        let mut consumed = 0;
        for found in self.find_iter(text) {
            out.extend(chars[consumed..found.start].iter());
            out.push_str(replacement);
            consumed = found.end;
        }
        out.extend(chars[consumed..].iter());
        out
    }

    /// The number of capturing groups.
    pub fn group_count(&self) -> usize {
        self.group_count
    }
}

fn rewrite_dotall(node: Node) -> Node {
    match node {
        Node::Any => Node::Class {
            negate: false,
            items: vec![ClassItem::DotAny],
        },
        Node::Group { index, inner } => Node::Group {
            index,
            inner: Box::new(rewrite_dotall(*inner)),
        },
        Node::Look { inner, positive } => Node::Look {
            inner: Box::new(rewrite_dotall(*inner)),
            positive,
        },
        Node::Concat(items) => Node::Concat(items.into_iter().map(rewrite_dotall).collect()),
        Node::Alt(items) => Node::Alt(items.into_iter().map(rewrite_dotall).collect()),
        Node::Repeat {
            inner,
            min,
            max,
            greedy,
        } => Node::Repeat {
            inner: Box::new(rewrite_dotall(*inner)),
            min,
            max,
            greedy,
        },
        other => other,
    }
}

impl Parser {
    fn peek(&self) -> Option<char> {
        self.chars.get(self.pos).copied()
    }

    fn alternation(&mut self) -> Result<Node, RegexError> {
        let mut branches = vec![self.sequence()?];
        while self.peek() == Some('|') {
            self.pos += 1;
            branches.push(self.sequence()?);
        }
        Ok(if branches.len() == 1 {
            branches.pop().expect("one branch")
        } else {
            Node::Alt(branches)
        })
    }

    fn sequence(&mut self) -> Result<Node, RegexError> {
        let mut items = Vec::new();
        while let Some(character) = self.peek() {
            if character == '|' || character == ')' {
                break;
            }
            items.push(self.quantified()?);
        }
        Ok(if items.len() == 1 {
            items.pop().expect("one item")
        } else {
            Node::Concat(items)
        })
    }

    fn quantified(&mut self) -> Result<Node, RegexError> {
        let atom = self.atom()?;
        let (min, max) = match self.peek() {
            Some('*') => {
                self.pos += 1;
                (0, UNBOUNDED)
            }
            Some('+') => {
                self.pos += 1;
                (1, UNBOUNDED)
            }
            Some('?') => {
                self.pos += 1;
                (0, 1)
            }
            Some('{') => {
                let save = self.pos;
                self.pos += 1;
                let mut low = String::new();
                while self.peek().is_some_and(|c| c.is_ascii_digit()) {
                    low.push(self.chars[self.pos]);
                    self.pos += 1;
                }
                if low.is_empty() {
                    self.pos = save;
                    return Ok(atom);
                }
                let high = if self.peek() == Some(',') {
                    self.pos += 1;
                    let mut text = String::new();
                    while self.peek().is_some_and(|c| c.is_ascii_digit()) {
                        text.push(self.chars[self.pos]);
                        self.pos += 1;
                    }
                    if text.is_empty() {
                        UNBOUNDED
                    } else {
                        text.parse::<usize>()
                            .map_err(|_| RegexError(format!("bad quantifier {{{text}}}")))?
                    }
                } else {
                    low.parse::<usize>()
                        .map_err(|_| RegexError(format!("bad quantifier {{{low}}}")))?
                };
                if self.peek() != Some('}') {
                    self.pos = save;
                    return Ok(atom);
                }
                self.pos += 1;
                let low = low
                    .parse::<usize>()
                    .map_err(|_| RegexError(format!("bad quantifier {{{low}}}")))?;
                (low, high)
            }
            _ => return Ok(atom),
        };
        let greedy = if self.peek() == Some('?') {
            self.pos += 1;
            false
        } else {
            true
        };
        Ok(Node::Repeat {
            inner: Box::new(atom),
            min,
            max,
            greedy,
        })
    }

    fn atom(&mut self) -> Result<Node, RegexError> {
        let character = self
            .peek()
            .ok_or_else(|| RegexError("pattern ended where an atom was expected".to_string()))?;
        match character {
            '(' => {
                self.pos += 1;
                let mut index = None;
                if self.chars.get(self.pos) == Some(&'?') {
                    match self.chars.get(self.pos + 1) {
                        Some(':') => self.pos += 2,
                        Some('=') => {
                            self.pos += 2;
                            let inner = self.alternation()?;
                            if self.peek() != Some(')') {
                                return Err(RegexError("unclosed lookahead".to_string()));
                            }
                            self.pos += 1;
                            return Ok(Node::Look {
                                inner: Box::new(inner),
                                positive: true,
                            });
                        }
                        Some('!') => {
                            self.pos += 2;
                            let inner = self.alternation()?;
                            if self.peek() != Some(')') {
                                return Err(RegexError("unclosed lookahead".to_string()));
                            }
                            self.pos += 1;
                            return Ok(Node::Look {
                                inner: Box::new(inner),
                                positive: false,
                            });
                        }
                        Some('P') if self.chars.get(self.pos + 2) == Some(&'<') => {
                            self.pos += 3;
                            let mut name = String::new();
                            while let Some(c) = self.peek() {
                                if c == '>' {
                                    break;
                                }
                                name.push(c);
                                self.pos += 1;
                            }
                            if self.peek() != Some('>') {
                                return Err(RegexError("unterminated group name".to_string()));
                            }
                            self.pos += 1;
                            self.group_count += 1;
                            index = Some(self.group_count);
                            self.names.insert(name, self.group_count);
                        }
                        other => {
                            return Err(RegexError(format!(
                                "unsupported group extension (?{}",
                                other.map(|c| c.to_string()).unwrap_or_default()
                            )));
                        }
                    }
                } else {
                    self.group_count += 1;
                    index = Some(self.group_count);
                }
                let inner = self.alternation()?;
                if self.peek() != Some(')') {
                    return Err(RegexError("unclosed group".to_string()));
                }
                self.pos += 1;
                Ok(Node::Group {
                    index,
                    inner: Box::new(inner),
                })
            }
            '[' => {
                self.pos += 1;
                self.class()
            }
            '.' => {
                self.pos += 1;
                Ok(Node::Any)
            }
            '^' => {
                self.pos += 1;
                Ok(Node::Start(self.multiline))
            }
            '$' => {
                self.pos += 1;
                Ok(Node::End(self.multiline))
            }
            '\\' => {
                self.pos += 1;
                self.escape(false)
            }
            '*' | '+' | '?' => Err(RegexError(format!(
                "nothing to repeat before {character:?}"
            ))),
            other => {
                self.pos += 1;
                Ok(Node::Char(self.fold_char(other)))
            }
        }
    }

    /// A pattern literal as the matcher sees it: ASCII-folded under `re.I`.
    fn fold_char(&self, character: char) -> char {
        if self.ignorecase {
            character.to_ascii_lowercase()
        } else {
            character
        }
    }

    fn escape(&mut self, in_class: bool) -> Result<Node, RegexError> {
        let character = self
            .peek()
            .ok_or_else(|| RegexError("pattern ended in an escape".to_string()))?;
        self.pos += 1;
        let item = match character {
            'd' => Some(ClassItem::Digit),
            'D' => Some(ClassItem::NotDigit),
            'w' => Some(ClassItem::Word),
            'W' => Some(ClassItem::NotWord),
            's' => Some(ClassItem::Space),
            'S' => Some(ClassItem::NotSpace),
            _ => None,
        };
        if in_class {
            return Ok(match item {
                Some(item) => Node::Class {
                    negate: false,
                    items: vec![item],
                },
                None => {
                    let character = escaped_char(character);
                    Node::Char(self.fold_char(character))
                }
            });
        }
        Ok(match character {
            'b' => Node::WordBoundary,
            'B' => Node::NotWordBoundary,
            'A' => Node::TextStart,
            'Z' => Node::TextEnd,
            _ => match item {
                Some(item) => Node::Class {
                    negate: false,
                    items: vec![item],
                },
                None => Node::Char(self.fold_char(escaped_char(character))),
            },
        })
    }

    fn class(&mut self) -> Result<Node, RegexError> {
        let negate = if self.peek() == Some('^') {
            self.pos += 1;
            true
        } else {
            false
        };
        let mut items = Vec::new();
        loop {
            let Some(character) = self.peek() else {
                return Err(RegexError("unclosed character class".to_string()));
            };
            if character == ']' {
                self.pos += 1;
                break;
            }
            if character == '\\' {
                self.pos += 1;
                let escaped = self
                    .peek()
                    .ok_or_else(|| RegexError("pattern ended in a class escape".to_string()))?;
                self.pos += 1;
                match escaped {
                    'd' => items.push(ClassItem::Digit),
                    'D' => items.push(ClassItem::NotDigit),
                    'w' => items.push(ClassItem::Word),
                    'W' => items.push(ClassItem::NotWord),
                    's' => items.push(ClassItem::Space),
                    'S' => items.push(ClassItem::NotSpace),
                    other => {
                        let folded = self.fold_char(escaped_char(other));
                        items.push(ClassItem::Range(folded, folded));
                    }
                }
                continue;
            }
            // A `-` is a range only when it is neither first nor last.
            if self.chars.get(self.pos + 1) == Some(&'-')
                && self.chars.get(self.pos + 2).is_some_and(|c| *c != ']')
            {
                let high = self.chars[self.pos + 2];
                items.push(ClassItem::Range(
                    self.fold_char(character),
                    self.fold_char(high),
                ));
                self.pos += 3;
                continue;
            }
            items.push(ClassItem::Range(
                self.fold_char(character),
                self.fold_char(character),
            ));
            self.pos += 1;
        }
        Ok(Node::Class { negate, items })
    }
}

/// The character a backslash escape stands for, as Python's `re` reads it.
///
/// Only the control escapes `re` defines are translated; every other escaped
/// character stands for itself (`.` stays `.`, `-` stays `-`), which is what
/// makes a pattern like `[^"\n]` mean "not a quote and not a newline" rather
/// than the literal `n` a hand-rolled scanner would read.
fn escaped_char(character: char) -> char {
    match character {
        'n' => '\n',
        't' => '\t',
        'r' => '\r',
        'f' => '\x0c',
        'v' => '\x0b',
        'a' => '\x07',
        '0' => '\0',
        other => other,
    }
}

fn class_matches(items: &[ClassItem], negate: bool, character: char) -> bool {
    let hit = items.iter().any(|item| match item {
        ClassItem::Range(low, high) => *low <= character && character <= *high,
        ClassItem::DotAny => true,
        ClassItem::Digit => is_digit(character),
        ClassItem::NotDigit => !is_digit(character),
        ClassItem::Word => is_word(character),
        ClassItem::NotWord => !is_word(character),
        ClassItem::Space => is_space(character),
        ClassItem::NotSpace => !is_space(character),
    });
    hit != negate
}

type Captures = Vec<Option<(usize, usize)>>;

/// Match `node` at `pos`, calling `k` on each successful end position. Returns
/// `true` when the continuation accepted; on `false` the captures are as they
/// were on entry.
fn run(
    node: &Node,
    text: &[char],
    pos: usize,
    caps: &mut Captures,
    k: &mut dyn FnMut(&mut Captures, usize) -> bool,
) -> bool {
    match node {
        Node::Char(expected) => {
            if text.get(pos) == Some(expected) {
                k(caps, pos + 1)
            } else {
                false
            }
        }
        Node::Any => {
            if pos < text.len() {
                k(caps, pos + 1)
            } else {
                false
            }
        }
        Node::Class { negate, items } => match text.get(pos) {
            Some(character) if class_matches(items, *negate, *character) => k(caps, pos + 1),
            _ => false,
        },
        Node::Start(multiline) => {
            if pos == 0 || (*multiline && text[pos - 1] == '\n') {
                k(caps, pos)
            } else {
                false
            }
        }
        Node::End(multiline) => {
            let at_end = pos == text.len();
            let before_a_final_newline = pos + 1 == text.len() && text.get(pos) == Some(&'\n');
            let at_a_line_end = *multiline && text.get(pos) == Some(&'\n');
            if at_end || before_a_final_newline || at_a_line_end {
                k(caps, pos)
            } else {
                false
            }
        }
        Node::TextStart => {
            if pos == 0 {
                k(caps, pos)
            } else {
                false
            }
        }
        Node::TextEnd => {
            if pos == text.len() {
                k(caps, pos)
            } else {
                false
            }
        }
        Node::WordBoundary | Node::NotWordBoundary => {
            let before = pos > 0 && is_word(text[pos - 1]);
            let after = pos < text.len() && is_word(text[pos]);
            let wanted = matches!(node, Node::WordBoundary);
            if (before != after) == wanted {
                k(caps, pos)
            } else {
                false
            }
        }
        Node::Group { index, inner } => match index {
            None => run(inner, text, pos, caps, k),
            Some(index) => {
                let index = *index;
                let start = pos;
                let saved = caps[index];
                let accepted = run(inner, text, pos, caps, &mut |inner_caps, end| {
                    let previous = inner_caps[index];
                    inner_caps[index] = Some((start, end));
                    if k(inner_caps, end) {
                        true
                    } else {
                        inner_caps[index] = previous;
                        false
                    }
                });
                if !accepted {
                    caps[index] = saved;
                }
                accepted
            }
        },
        Node::Look { inner, positive } => {
            // The lookahead runs on a copy of the captures: it is an assertion
            // about the text, not a match that may leave a group behind.
            let mut probe = caps.clone();
            let matched = run(inner, text, pos, &mut probe, &mut |_probe, _end| true);
            if matched == *positive {
                k(caps, pos)
            } else {
                false
            }
        }
        Node::Concat(items) => concat(items, text, pos, caps, k),
        Node::Alt(branches) => {
            for branch in branches {
                let saved = caps.clone();
                if run(branch, text, pos, caps, k) {
                    return true;
                }
                *caps = saved;
            }
            false
        }
        Node::Repeat {
            inner,
            min,
            max,
            greedy,
        } => repeat(inner, text, pos, caps, 0, *min, *max, *greedy, k),
    }
}

fn concat(
    items: &[Node],
    text: &[char],
    pos: usize,
    caps: &mut Captures,
    k: &mut dyn FnMut(&mut Captures, usize) -> bool,
) -> bool {
    match items.split_first() {
        None => k(caps, pos),
        Some((first, rest)) => {
            let saved = caps.clone();
            let accepted = run(first, text, pos, caps, &mut |inner_caps, end| {
                let checkpoint = inner_caps.clone();
                if concat(rest, text, end, inner_caps, k) {
                    true
                } else {
                    *inner_caps = checkpoint;
                    false
                }
            });
            if !accepted {
                *caps = saved;
            }
            accepted
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn repeat(
    inner: &Node,
    text: &[char],
    pos: usize,
    caps: &mut Captures,
    count: usize,
    min: usize,
    max: usize,
    greedy: bool,
    k: &mut dyn FnMut(&mut Captures, usize) -> bool,
) -> bool {
    let can_expand = count < max;
    let satisfied = count >= min;
    if greedy {
        if can_expand {
            let saved = caps.clone();
            let expanded = run(inner, text, pos, caps, &mut |inner_caps, end| {
                if end == pos {
                    // An empty repetition cannot make progress; expanding again
                    // would not terminate.
                    return false;
                }
                let checkpoint = inner_caps.clone();
                if repeat(inner, text, end, inner_caps, count + 1, min, max, greedy, k) {
                    true
                } else {
                    *inner_caps = checkpoint;
                    false
                }
            });
            if expanded {
                return true;
            }
            *caps = saved;
        }
        if satisfied {
            return k(caps, pos);
        }
        false
    } else {
        if satisfied && k(caps, pos) {
            return true;
        }
        if !can_expand {
            return false;
        }
        let saved = caps.clone();
        let expanded = run(inner, text, pos, caps, &mut |inner_caps, end| {
            if end == pos {
                return false;
            }
            let checkpoint = inner_caps.clone();
            if repeat(inner, text, end, inner_caps, count + 1, min, max, greedy, k) {
                true
            } else {
                *inner_caps = checkpoint;
                false
            }
        });
        if !expanded {
            *caps = saved;
        }
        expanded
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_patterns_the_port_depends_on_match_pythons_way() {
        let bound =
            Regex::new(r#"<text class="bound-label"([^>]*)>(.*?)</text>"#, true).expect("compiles");
        let markup = "<text class=\"bound-label\" x=\"1\" y=\"2\">a</text>middle\
                      <text class=\"bound-label\" x=\"3\" y=\"4\"><title>t</title>b</text>";
        let found = bound.find_all(markup);
        assert_eq!(found.len(), 2);
        assert_eq!(found[0][0].as_deref(), Some(" x=\"1\" y=\"2\""));
        assert_eq!(found[0][1].as_deref(), Some("a"));
        assert_eq!(found[1][1].as_deref(), Some("<title>t</title>b"));

        let lazy = Regex::new(r"<title>.*?</title>", true).expect("compiles");
        assert_eq!(
            lazy.replace_all("<title>a</title>x<title>b</title>", ""),
            "x"
        );

        let tick = Regex::new(
            r#"<text x="[-0-9.]+" y="276" text-anchor="middle">([^<]*)</text>"#,
            false,
        )
        .expect("compiles");
        let one = tick.find_all("<text x=\"1.0\" y=\"276\" text-anchor=\"middle\">2.5</text>");
        assert_eq!(one[0][0].as_deref(), Some("2.5"));

        let alternation =
            Regex::new(r"\[\s*\]|\(\s*\)|\bNone\b|\bnull\b|\bnan\b", false).expect("compiles");
        assert!(alternation.is_match("has [] here"));
        assert!(alternation.is_match("None"));
        assert!(!alternation.is_match("Nonexistent"));
        assert!(!alternation.is_match("none"));

        let named = Regex::new(r#"<desc class="panel-summary">(?P<body>.*?)</desc>"#, true)
            .expect("compiles");
        let found = named
            .search("<svg><desc class=\"panel-summary\">{}</desc></svg>")
            .expect("matches");
        assert_eq!(found.named("body").as_deref(), Some("{}"));

        let count = Regex::new(r#"<circle class="sample""#, false).expect("compiles");
        assert_eq!(
            count
                .find_all("<circle class=\"sample\"a<circle class=\"sample\"")
                .len(),
            2
        );

        let statement = Regex::new(
            r#"bound "(?P<label>[^"]*)" at (?P<value>[-+0-9.eE]+) on axis (?P<low>[-+0-9.eE]+)\.\.(?P<high>[-+0-9.eE]+)"#,
            false,
        )
        .expect("compiles");
        let found = statement
            .search("bound \"x\" at 1 on axis 2..3")
            .expect("matches");
        assert_eq!(found.named("low").as_deref(), Some("2"));

        let digits = Regex::new(r"^(?P<key>[A-Za-z_][A-Za-z0-9_]*)=(?P<value>\S+)$", false)
            .expect("compiles");
        assert!(digits.is_match("hostile_p99_guard=900"));
        assert!(!digits.is_match("hostile_p99_guard =900"));

        let range = Regex::new(r"[A-Za-z][\w-]*", false).expect("compiles");
        assert_eq!(
            range.find_all("governs hostile_p99 x-y")[0][0].as_deref(),
            Some("governs")
        );
        let counted = Regex::new(r"[0-9]+(?:\.[0-9]+)?", false).expect("compiles");
        assert_eq!(counted.find_all("1.5x2")[0][0].as_deref(), Some("1.5"));
    }

    #[test]
    fn a_negated_class_is_the_negation_it_says_it_is() {
        // Vacuity: the same pattern text with the negation removed answers
        // differently on the same input, so the test measures the negation and
        // not the engine's willingness to match something.
        let positive = Regex::new(r"[^>]*", false).expect("compiles");
        let against = Regex::new(r"[>]*", false).expect("compiles");
        assert_eq!(positive.find_all("ab>cd")[0][0].as_deref(), Some("ab"));
        assert_eq!(against.find_all("ab>cd")[0][0].as_deref(), Some(""));
    }

    #[test]
    fn positions_are_character_offsets_so_a_slice_is_the_matched_text() {
        let pattern = Regex::new(r"[0-9]+", false).expect("compiles");
        let found = pattern.search("±12").expect("matches");
        assert_eq!(found.start, 1);
        assert_eq!(found.end, 3);
        assert_eq!(found.group(0).as_deref(), Some("12"));
    }

    #[test]
    fn a_control_escape_is_the_character_python_reads_it_as() {
        // The checker's patterns spell a newline `\n`; a scanner that read the
        // escape as the literal letter `n` would make `[^"\n]` mean "not a
        // quote and not an n", which is what a string-literal scan did before
        // this engine translated it. The pair below is the vacuity: the same
        // pattern with the escape removed answers differently on the same
        // input, so the test measures the translation rather than the engine's
        // willingness to match.
        let escaped = Regex::new(r#""([^"\n]*)""#, false).expect("compiles");
        let literal = Regex::new(r#""([^"n]*)""#, false).expect("compiles");
        let text = "\"alpha\",\n    \"banana\"";
        let group = |regex: &Regex| -> Vec<Option<String>> {
            regex
                .find_all(text)
                .into_iter()
                .map(|groups| groups[0].clone())
                .collect()
        };
        assert_eq!(
            group(&escaped),
            vec![Some("alpha".to_string()), Some("banana".to_string())]
        );
        assert_ne!(group(&escaped), group(&literal));
    }

    #[test]
    fn ignore_case_reports_the_documents_own_spelling() {
        let folding = Regex::new_with_flags(
            r"\b(one|two|three) producers",
            Flags {
                ignorecase: true,
                ..Flags::default()
            },
        )
        .expect("compiles");
        let found = folding
            .search("Two producers are declared")
            .expect("matches");
        assert_eq!(found.group(1).as_deref(), Some("Two"));
        // Vacuity: without `re.I` the same pattern does not match the same
        // text at all, so the case measures the fold and not a permissive scan.
        let strict = Regex::new(r"\b(one|two|three) producers", false).expect("compiles");
        assert!(!strict.is_match("Two producers are declared"));
    }

    #[test]
    fn multiline_anchors_a_line_but_a_text_anchor_does_not() {
        let line = Regex::new_with_flags(
            r"^\*\*Read report-only output",
            Flags {
                multiline: true,
                ..Flags::default()
            },
        )
        .expect("compiles");
        assert!(line.is_match("intro\n**Read report-only output; do not rely"));
        let strict = Regex::new(r"^\*\*Read report-only output", false).expect("compiles");
        assert!(!strict.is_match("intro\n**Read report-only output; do not rely"));
        let text_anchor = Regex::new(r"\Aabc", false).expect("compiles");
        assert!(!text_anchor.is_match("intro\nabc"));
    }

    #[test]
    fn an_anchor_and_a_greedy_repeat_take_the_whole_line() {
        let gap = Regex::new(r"no sample gap(?![ \w])", false).expect("compiles");
        assert!(gap.is_match("no sample gap"));
        assert!(gap.is_match("no sample gap;"));
        assert!(!gap.is_match("no sample gap over 0.5 s"));
        assert!(!gap.is_match("no sample gaps"));

        let whole = Regex::new(r"^(.*)$", false).expect("compiles");
        assert_eq!(whole.find_all("a b c")[0][0].as_deref(), Some("a b c"));
        let lazy = Regex::new(r"^(.*?)$", false).expect("compiles");
        assert_eq!(lazy.find_all("a b c")[0][0].as_deref(), Some("a b c"));
    }
}
