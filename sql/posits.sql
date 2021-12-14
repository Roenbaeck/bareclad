-- Query that can be run against the pesisted SQLite database in order to list all posits
-- in human readable format
WITH RECURSIVE AppearanceSet_Members(AppearanceSet_Identity, Appearance_Identity, AppearanceSet_Appearance_Identities) AS (
    SELECT 
    	AppearanceSet_Identity, 
    	'', 
    	AppearanceSet_Appearance_Identities || ',' 
    FROM "AppearanceSet"
    UNION ALL 
    SELECT
        AppearanceSet_Identity,
        substr(AppearanceSet_Appearance_Identities, 0, instr(AppearanceSet_Appearance_Identities, ',')),
        substr(AppearanceSet_Appearance_Identities, instr(AppearanceSet_Appearance_Identities, ',') + 1)
    FROM "AppearanceSet_Members" 
    WHERE AppearanceSet_Appearance_Identities != ''
), flattened_AppearanceSet AS (
	SELECT 
		m.AppearanceSet_Identity, 
		group_concat('(' || r.Role || ', ' || a.Thing_Identity || ')', ', ') AS AppearanceSet
	FROM "AppearanceSet_Members" m
	JOIN "Appearance" a 
	ON a.Appearance_Identity = m.Appearance_Identity
	JOIN "Role" r 
	ON r.Role_Identity = a.Role_Identity
	WHERE m.Appearance_Identity != ''
	GROUP BY m.AppearanceSet_Identity
)
SELECT p.Posit_Identity || ' = [{' || s.AppearanceSet || '}, "' || p.AppearingValue || '", ' || p.AppearanceTime || ']'
FROM "Posit" p
JOIN "flattened_AppearanceSet" s 
ON s.AppearanceSet_Identity = p.AppearanceSet_Identity;
