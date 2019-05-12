### SQL Server Grammar

These are the grammar files and antlr generated python files for the T-SQL grammar.

You can regenerate this with:

$ antlr4 -Dlanguage=Python2 TSqlLexer.g4
$ antlr4 -Dlanguage=Python2 TSqlParser.g4
