-- Query that can be run against the pesisted SQLite database in order to list all posits
-- in human readable format
-- Query that can be run against the pesisted SQLite database in order to list all posits
-- in human readable format
SELECT 
	Posit_Identity || 
	' [{(' || replace(replace(AppearanceSet, ',', ', '), '|', '), (') || ')}, "' || 
	AppearingValue || '", ' || 
	AppearanceTime || ']' || ' ' || 
	ValueType || ', ' ||
	TimeType AS Posit
FROM "Posit" 
ORDER BY Posit_Identity;
