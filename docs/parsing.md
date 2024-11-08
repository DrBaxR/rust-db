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

This is what I am currently researching: 
- https://www.youtube.com/watch?v=8nBoVjEOCMI&t=655s
- https://www.youtube.com/watch?v=OIKL6wFjFOo&list=PLBlnK6fEyqRgPLTKYaRhcMt8pVKl4crr6

# Resources
1. https://marianogappa.github.io/software/2019/06/05/lets-build-a-sql-parser-in-go/
2. https://en.wikipedia.org/wiki/Finite-state_machine
3. https://en.wikipedia.org/wiki/Lexical_analysis#Tokenization
4. https://doxygen.postgresql.org/dir_9bdbe319e7c10dca287b6ea9b66ff88a.html