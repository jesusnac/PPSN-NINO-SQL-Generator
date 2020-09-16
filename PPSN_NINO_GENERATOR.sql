CREATE VIEW UVW_NEWID
AS SELECT 
    ABS(CAST(CAST(NEWID() AS VARBINARY) AS INT)) AS RANDOM_ID,
    RIGHT(REPLICATE(N'0',10) + CAST(ABS(CAST(CAST(NEWID() AS VARBINARY) AS INT)) AS NVARCHAR(10)), 10) AS RANDOM_ID_CHAR
;
GO

CREATE FUNCTION UFN_TBL_AUX_PPSN
                (@NO_THOUSANDS TINYINT = 1)
RETURNS TABLE
AS
RETURN
WITH E1(COL) AS
         (SELECT COL FROM (VALUES (1), (1), (1), (1), (1), (1), (1), (1), (1), (1)) T1(COL)),  -- 10 rows
     E2(COL) AS
         (SELECT 1 FROM E1 CROSS JOIN E1 AS E1B), -- 10*10 rows
     E3(COL) AS
         (SELECT 1 FROM E1 CROSS JOIN E2), -- 10*100 rows
     F1(COL) AS
         (SELECT 1 FROM(SELECT TOP (ISNULL(@NO_THOUSANDS, 1)) 1 AS PPSN FROM E1) AS E1C(COL) CROSS JOIN E3), -- 1000 * @NO_THOUSANDS
     F2(PPSNUMBER, PPSLETTER) AS
         (SELECT RIGHT((SELECT RANDOM_ID FROM UVW_NEWID), 7) AS PPSNUMBER,
                CHAR(65 + (SELECT RANDOM_ID FROM UVW_NEWID) % 26) AS PPSLETTER
         FROM F1),
     F3(PPSNUMBER, PPSLETTER, PPSCHECK) AS
         (SELECT PPSNUMBER,
                PPSLETTER,
                CHAR(64 + (CAST(SUBSTRING(PPSNUMBER, 1, 1) AS TINYINT) * 8 + CAST(SUBSTRING(PPSNUMBER, 2, 1) AS TINYINT) * 7 + CAST(SUBSTRING(PPSNUMBER, 3, 1) AS TINYINT) * 6 + CAST(SUBSTRING(PPSNUMBER, 4, 1) AS TINYINT) * 5 + CAST(SUBSTRING(PPSNUMBER, 5, 1) AS TINYINT) * 4 + CAST(SUBSTRING(PPSNUMBER, 6, 1) AS TINYINT) * 3 + CAST(SUBSTRING(PPSNUMBER, 7, 1) AS TINYINT) * 2 + (ASCII(PPSLETTER) - 64) * 9) % 23) AS PPSCHECK
         FROM F2)
     SELECT PPSNUMBER + CASE
                            WHEN ASCII(PPSCHECK) = 64
                            THEN N'W'
                            ELSE PPSCHECK
                        END + PPSLETTER AS PPSN
     FROM F3;
GO

CREATE FUNCTION UFN_TBL_AUX_NINO
    (@NO_THOUSANDS TINYINT = 1,
     @STYLE        TINYINT = 1)
RETURNS @TBL_AUX_NINO TABLE(NINO VARCHAR(13))
AS
BEGIN

    --DECLARE @STYLE TINYINT = 1
    --DECLARE @NO_THOUSANDS TINYINT = 1
    DECLARE @TBL_GROUP_1 TABLE(GROUP_VALUE CHAR(2));
    DECLARE @TBL_GROUP_5 TABLE(CHAR_VALUE CHAR(1));

    DECLARE @GROUP_1_COUNT INT;
    DECLARE @MULTIPLY_FACTOR INT;

    SET @STYLE = ISNULL(@STYLE, 1);
    IF (@STYLE < 1 OR @STYLE > 2)
    BEGIN
        SET @STYLE = 1;
    END;

    INSERT INTO @TBL_GROUP_1(GROUP_VALUE)
    SELECT F.FIRST_CHAR + S.SECOND_CHAR AS GROUP_1
    FROM(VALUES('A'), ('B'), ('C'), ('E'), ('G'), ('H'), ('J'), ('K'), ('L'), ('M'), ('N'), ('O'), ('P'), ('R'), ('S'), ('T'), ('W'), ('X'), ('Y'), ('Z')) AS F(FIRST_CHAR)
        CROSS JOIN(VALUES('A'), ('B'), ('C'), ('E'), ('G'), ('H'), ('J'), ('K'), ('L'), ('M'), ('N'), ('P'), ('R'), ('S'), ('T'), ('W'), ('X'), ('Y'), ('Z')) AS S(SECOND_CHAR)
    WHERE(FIRST_CHAR + SECOND_CHAR) NOT IN(SELECT INVALID_COMBINATION
                                           FROM(VALUES('GB'), ('BG'), ('NK'), ('KN'), ('TN'), ('NT'), ('ZZ')) AS NOT_ALLOWED(INVALID_COMBINATION));

    SET @GROUP_1_COUNT = (SELECT COUNT(1) FROM @TBL_GROUP_1);
    SET @MULTIPLY_FACTOR = CEILING((@NO_THOUSANDS * 1000.00) / @GROUP_1_COUNT);

    WITH A1(ROW_NO)
         AS (SELECT 1 AS ROW_NO
             UNION ALL
             SELECT ROW_NO + 1 FROM A1 WHERE ROW_NO < @MULTIPLY_FACTOR),
         A2(GROUP_VALUE,
            ROW_ID)
         AS (SELECT TOP (@NO_THOUSANDS * 1000) GROUP_VALUE,
                                               ROW_NUMBER() OVER(
                                               ORDER BY RANDOM_ID) AS ROW_ID
             FROM @TBL_GROUP_1
                  CROSS JOIN UVW_NEWID
                  CROSS JOIN A1),
         B1(ROW_NO)
         AS (SELECT 1 AS [NO]
             UNION ALL
             SELECT ROW_NO + 1
             FROM B1
             WHERE ROW_NO < CEILING((@NO_THOUSANDS * 1000.00) / 5)),
         B2(GROUP_VALUE,
            ROW_ID)
         AS (SELECT TOP (@NO_THOUSANDS * 1000) CHAR_VALUE AS GROUP_VALUE,
                                               ROW_NUMBER() OVER(
                                               ORDER BY RANDOM_ID) AS ROW_ID
             FROM(VALUES('A'), ('B'), ('C'), ('D'), ('')) AS GROUP_5(CHAR_VALUE)
                 CROSS JOIN B1
                 CROSS JOIN UVW_NEWID),
         E1(COL)
         AS (SELECT COL
             FROM(VALUES(1), (1), (1), (1), (1), (1), (1), (1), (1), (1)) AS T1(COL)), -- 10 rows
         E2(COL)
         AS (SELECT 1 FROM E1
                           CROSS JOIN E1 AS E1B), -- 10*10 rows
         E3(COL)
         AS (SELECT 1 FROM E1
                           CROSS JOIN E2), -- 10*100 rows
         F1(COL,
            ROW_ID)
         AS (SELECT RIGHT((SELECT RANDOM_ID FROM UVW_NEWID), 6),
                    ROW_NUMBER() OVER(
                    ORDER BY(SELECT NULL)) AS ROW_ID
             FROM E2
                  CROSS JOIN E3), -- 1000 * @NO_THOUSANDS
         F2(GROUP_1,
            GROUP_2_4,
            GROUP_5,
            NINO)
         AS (SELECT A2.GROUP_VALUE AS GROUP_1,
                    COL AS GROUP_2_4,
                    B2.GROUP_VALUE AS GROUP_5,
                    CASE @STYLE
                        WHEN 1
                        THEN A2.GROUP_VALUE + COL + B2.GROUP_VALUE
                        WHEN 2
                        THEN A2.GROUP_VALUE + ' '
                            + LEFT(COL, 2) + ' '
                            + SUBSTRING(COL, 3, 2) + ' ' 
                            + RIGHT(COL, 2) + CASE B2.GROUP_VALUE
                                                  WHEN ' '
                                                  THEN ''
                                                  ELSE ' ' + B2.GROUP_VALUE
                                              END
                        ELSE ''
                    END AS NINO
             FROM F1
                  INNER JOIN A2 ON F1.ROW_ID = A2.ROW_ID
                  INNER JOIN B2 ON F1.ROW_ID = B2.ROW_ID)
         INSERT INTO @TBL_AUX_NINO(NINO)
         SELECT NINO FROM F2 OPTION(MAXRECURSION 0);
    RETURN;
END;