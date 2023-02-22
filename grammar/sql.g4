grammar sql;

@header {
    package vsb.baca.grammar;
}

SELECT:			'SELECT';
FROM:			'FROM';
JOIN:			'JOIN';
LEFT:			'LEFT';
RIGHT:			'RIGHT';
FULL:			'FULL';
OUTER:			'OUTER';
INNER:			'INNER';
ON:				'ON';
USING:			'USING';
WHERE:			'WHERE';
OVER:			'OVER';
PARTITION:	    'PARTITION';
ORDER:  		'ORDER';
BY:				'BY';
FOLLOWING:		'FOLLOWING';
PRECEDING:		'PRECEDING';
UNBOUNDED:		'UNBOUNDED';
CURRENT:		'CURRENT';
ROW:			'ROW';
RANGE:			'RANGE';
ROWS:			'ROWS';
BETWEEN:		'BETWEEN';
AND:			'AND';
ID : [a-zA-Z,.*<>=!:]+ ; // match identifiers
WS : [ \t\r\n]+ -> skip ; // skip spaces, tabs, newlines, \r (Windows)
LR_BRACKET:     '(';
RR_BRACKET:     ')';

sql : SELECT ID* from where? ;

from: FROM table_expr join*;

table_expr: ID* |
       LR_BRACKET sql RR_BRACKET ID?;

join: join_prefix table_expr ON ID* |
      join_prefix table_expr USING ID* ;

join_prefix : LEFT OUTER? | RIGHT OUTER? | FULL OUTER? | INNER;

where: WHERE conditional_expr;

conditional_expr: ID* | LR_BRACKET sql RR_BRACKET ID*;