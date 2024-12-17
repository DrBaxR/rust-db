# Parsing

There is a dedicated component in the system that is responsible of taking a SQL string as input and spittig out and AST, which is then fed to the rest of the system - this component is called the parser.

The parser has to go through two stages in order to produce its output (which is an *AST*):
1. Lexical analysis
2. Syntax Analysis

## Lexical Analysis

The lexical analysis step's main goal is to take a string (or list, if you will) of characters as input and produce a list of tokens as output. These tokens are an intermediary representation of the language's elements that is used by the syntax analysis step, and they are split into a bunch of categories:
- data types: `INTEGER`, `VARCHAR` etc.
- delimiters: `,`, `.`, `(` etc.
- functions: `MIN`, `SUM`, `COUNT` etc.
- keywords: `SELECT`, `AS`, `VALUES` etc.
- operators: `+`, `/`, `AND` etc.
- values: `1`, `'strings'` etc.

### Implementation

This is the general idea of what the lexical analysis step does, and now I'm going to describe how I implemented my lexer. The overview of the implementation would be that I implemented a tokenizer for each category of tokens (`DataTypeTokenizer`, `DelimiterTokenizer` etc.), which all have a common interface: a `largest_match(&self, raw: &str) -> Option<(Token, usize)>` method. This method takes in a string and returns the largest matching token of the tokenizer's category from the start of the string AND the length in characters of said token (alternatively, it will return `None` if there is no match).

For example, the `largest_match` for a *data type* in the string `TIMESTAMP_123` would be the token `DataType::Timestamp`, with a length of `9`. Note that the token `DataType::Time` also matches the input, however its length is smaller than the timestamp token's, therefore it is not picked.

All the tokenizers I implemented fit in one of two categories (don't let the names of the category names fool you, they both use FSM's under the hood):
1. Character Sequence
2. FSM

One limitation that I won't go into detail about that is imposed on all the tokenizers is that they **NEED** to be usable on a character-by-character basis (which is the reason why they all use FSM's that switch states based on characters received).

#### Character Sequence

The idea behind this type of parsers is that most of the tokenizers just boil down to matching pre-defined set of character sequences to a token, therefore they can be use a higher-level tokenizer for their implementation.

The way the higher level tokenizer works is that you can feed it a bunch of strings and match those strings to a specific `Token` entity before using it. After the setup stage, you can convert it to an entity that exposes a FSM interface. The implementation of this tokenizer can be described as a modified **trie**, which leads to `O(n)` complexity for matching a string to a token.

#### FSM

This category is just using a tailor-made FSM for the tokenizer in order to match a character sequence to a token. This category is used for token categories that don't fit the description provided for the character sequence tokenizer: there *isn't* a finite number of values for the tokens - a good example for this is the data type tokenizer (here some of the valid values for tokens are `1`, `2`, `3`, ...).

The only thing worth mentioning about this type of tokenizer is that is made up of two stages:
1. **Scanning**: Determining the category of the token (integer, string, float etc.)
2. **Evaluating**: Parsing the value of the string (i.e. converting the string `"123"` to the integer `123`)

## Syntax Analysis

Syntax analysis is also called parsing and it takes in a stream of tokens and spits out an AST. The structure of the AST's nodes depends on the language that is being parsed.

There are multiple ways to tackle this problem, which leands to there being different types of parsers. I have chosen to go for a *recursive descent parser*, because it's simple to implement, however if I were to pick a production-ready solution, I'd go for a *LR parser* (either using `yacc` or `bison`).

The grammar of the language is represented in BNF (which in this case is a subset of the SQL specification) or in other ways of representing a grammar. Once you have the language representation, you generally follow this three-step process:
1. **Read tokens** (not necessarily one-by-one)
2. **Apply grammar rules** (which can be found in the grammar specification)
3. **Build AST nodes** (which may lead to **errors** if the syntax was not followed)

The things that need to be considered here:
1. Representing the language in BNF
2. Defining the structure of the AST's nodes
3. Implementing the grammar rules (translating them from the BNF representation to code)

### Implementation

A high-level explaination of how the parser is implemented is that the `SqlParser` entity, which exposes the `parse(sql: String)` method uses some lower level parsers, one for each of the SQL statement types (select, insert, update etc.) to determine if the string (it's not really a string, but a list of tokens prodiced by the lexical analysis step) can be parsed into any of the statements. If none match, then an error is returned.

**Note:** After producing the AST, the BusTub codebase uses a *binder*, which takes in a PSQL AST and "binds" it (no clue what that means at the moment).

# Resources
These are the resources I used while researching this topic:
1. https://marianogappa.github.io/software/2019/06/05/lets-build-a-sql-parser-in-go/
2. https://en.wikipedia.org/wiki/Finite-state_machine
3. https://en.wikipedia.org/wiki/Lexical_analysis#Tokenization
4. https://doxygen.postgresql.org/dir_9bdbe319e7c10dca287b6ea9b66ff88a.html
5. https://www.youtube.com/watch?v=8nBoVjEOCMI&t=655s
6. https://en.wikipedia.org/wiki/Recursive_descent_parser
7. https://forcedotcom.github.io/phoenix/