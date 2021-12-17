-- Query that can be run against the pesisted SQLite database in order to list all posits
-- in human readable format
WITH RECURSIVE Appearances(Posit_Identity, Appearance, AppearanceSet) AS (
    SELECT 
    	Posit_Identity, 
    	'', 
    	AppearanceSet || '|' 
    FROM "Posit"
    UNION ALL 
    SELECT
        Posit_Identity,
        substr(AppearanceSet, 0, instr(AppearanceSet, '|')),
        substr(AppearanceSet, instr(AppearanceSet, '|') + 1)
    FROM "Appearances" 
    WHERE AppearanceSet != ''
), ResolvedAppearanceSet(Posit_Identity, AppearanceSet) AS (
	SELECT 
		Posit_Identity, 
		group_concat(
			'(' || 
			substr(Appearance, 0, instr(Appearance, ',')) || 
			', ' || 
			r."Role" || 
			')', 
		', ') AS AppearanceSet
	FROM "Appearances" a
	JOIN "Role" r
	  ON r.Role_Identity = substr(Appearance, instr(Appearance, ',') + 1)
	WHERE Appearance != ''
	GROUP BY Posit_Identity
)
SELECT 
	s.Posit_Identity || 
	' [{' || s.AppearanceSet || '}, "' || 
	p.AppearingValue || '", ' || 
	p.AppearanceTime || ']' || ' ' || 
	p.ValueType || ', ' ||
	p.TimeType AS Posit
FROM ResolvedAppearanceSet s
JOIN Posit p 
  ON p.Posit_Identity = s.Posit_Identity
ORDER BY s.Posit_Identity;
