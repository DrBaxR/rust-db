# Parsing

There is a dedicated component in the system that is responsible of taking a SQL string as input and spittig out and AST, which is then fed to the rest of the system - this component is called the parser.

The parser has to go through two stages in order to produce its output (which is an *AST*):
1. Lexical analysis
2. Syntax Analysis

## Lexical Analysis

TODO: describe high level idea of how I implemented the tokenizer
TODO: ennumerate all sub-tokenizers
TODO: explain general character sequence matcher
TODO: explain specified FSM parser example (i'd say the value one, since it's more complex)
TODO: mention the two stages of the lexing: scanning and evaluating

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