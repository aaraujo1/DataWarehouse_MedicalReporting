/*------------------------------------*/
/*---- Insert to Facilities table ----*/
/*------------------------------------*/

-- insert into Facilities table
begin try
    -- begin try
    begin tran
        -- begin transaction


        insert into FacilityMeasures_US.Facilities(FacilityCode,
                                                   FacilityName,
                                                   Address,
                                                   City,
                                                   State,
                                                   ZipCode,
                                                   CountyName,
                                                   PhoneNumber)
        select distinct FacilityID,
                        FacilityName,
                        Address,
                        City,
                        State,
                        ZipCode,
                        CountyName,
                        PhoneNumber
        from DataImport.Complications_and_Deaths
        union
        select distinct FacilityID,
                        FacilityName,
                        Address,
                        City,
                        State,
                        ZipCode,
                        CountyName,
                        PhoneNumber
        from DataImport.Healthcare_Associated_Infections
    commit; -- if no errors, commit

end try
begin catch
    rollback; -- if errors, rollback
end catch
go;

/*--------------------------------------*/
/*---- Insert to MeasureTypes table ----*/
/*--------------------------------------*/
insert into FacilityMeasures_US.MeasureTypes (MeasureTypeName)
values ('Complications and Deaths'),
       ('Healthcare Associated Infections');
go;

/*------------------------------------------*/
/*---- Insert to MeasureSortOrder table ----*/
/*------------------------------------------*/
insert into FacilityMeasures_US.MeasureSortOrder (MeasureSortOrderName)
values ('Descending - Largest Score Is High Rank'), -- excel rank() function: 0 for descending
       ('Ascending - Largest Score Is Low Rank'), -- excel rank() function: 1 for ascending
       ('Unknown');
go;

/*----------------------------------*/
/*---- Insert to Measures table ----*/
/*----------------------------------*/

-- insert into Measures table
begin try
    -- begin try
    begin tran
        -- begin transaction


        insert into FacilityMeasures_US.Measures(MeasureCode,
                                                 MeasureName,
                                                 MeasureTypeID,
                                                 SortOrderID,
                                                 NationalScoreMax,
                                                 NationalScoreMin,
                                                 NationalScoreAverage)
            -- cd
        select cd.MeasureID,
               cd.MeasureName,
               1,
               so.SortOrderID, -- change
               max(iif(cd.Score = 'Not Available' or cd.Score = '--', null, cast(cd.Score as decimal(10, 3)))),
               min(iif(cd.Score = 'Not Available' or cd.Score = '--', null, cast(cd.Score as decimal(10, 3)))),
               cast(avg(iif(cd.Score = 'Not Available' or cd.Score = '--', null,
                            cast(cd.Score as decimal(10, 3)))) as decimal(10, 3))
        from DataImport.Complications_and_Deaths cd
                 left join (
            select distinct cd.MeasureName,
                            b.AverageScoreBetter,
                            w.AverageScoreWorse,
                            no.AverageScoreNoDifferent,
                            iif(b.AverageScoreBetter is not null and w.AverageScoreWorse is not null,
                                iif(b.AverageScoreBetter > w.AverageScoreWorse,
                                    0, -- Largest Score Is High Rank
                                    1 --Largest Score Is Low Rank
                                    ),
                                iif(b.AverageScoreBetter is null,
                                    iif(no.AverageScoreNoDifferent > w.AverageScoreWorse,
                                        0, -- Largest Score Is High Rank
                                        1 --Largest Score Is Low Rank
                                        ),
                                    iif(w.AverageScoreWorse is null,
                                        iif(b.AverageScoreBetter > no.AverageScoreNoDifferent,
                                            0, -- Largest Score Is High Rank
                                            1 --Largest Score Is Low Rank
                                            ),
                                        2 -- score sort order is unknown
                                        )
                                    )
                                ) as SortOrderID
            from DataImport.Complications_and_Deaths cd

                     left join
                 (
                     select cd.MeasureName,
                            avg(iif(cd.Score = 'Not Available' or cd.Score = '--', null,
                                    cast(cd.Score as decimal(10, 3)))
                                ) as AverageScoreBetter,
                            cd.ComparedToNational
                     from DataImport.Complications_and_Deaths cd
                     where cd.ComparedToNational = 'Better than the National Benchmark'
                        or cd.ComparedToNational = 'Better Than the National Rate'
                        or cd.ComparedToNational = 'Better Than the National Value'
                     group by cd.MeasureName, cd.ComparedToNational
                 ) b on b.MeasureName = cd.MeasureName
                     left join
                 (
                     select cd.MeasureName,
                            avg(iif(cd.Score = 'Not Available' or cd.Score = '--', null,
                                    cast(cd.Score as decimal(10, 3)))
                                ) as AverageScoreWorse,
                            cd.ComparedToNational
                     from DataImport.Complications_and_Deaths cd
                     where cd.ComparedToNational = 'Worse than the National Benchmark'
                        or cd.ComparedToNational = 'Worse Than the National Rate'
                        or cd.ComparedToNational = 'Worse Than the National Value'
                     group by cd.MeasureName, cd.ComparedToNational
                 ) w on w.MeasureName = cd.MeasureName
                     left join
                 (
                     select cd.MeasureName,
                            avg(iif(cd.Score = 'Not Available' or cd.Score = '--', null,
                                    cast(cd.Score as decimal(10, 3)))
                                ) as AverageScoreNoDifferent,
                            cd.ComparedToNational
                     from DataImport.Complications_and_Deaths cd
                     where cd.ComparedToNational = 'No Different than the National Benchmark'
                        or cd.ComparedToNational = 'No Different Than the National Rate'
                        or cd.ComparedToNational = 'No Different Than the National Value'
                     group by cd.MeasureName, cd.ComparedToNational
                 ) no on no.MeasureName = cd.MeasureName
        ) as so on so.MeasureName = cd.MeasureName
        group by cd.MeasureID, cd.MeasureName, SortOrderID

                 -- union
        union

        -- hai
        select hai.MeasureID,
               hai.MeasureName,
               2,
               so.SortOrderID, -- change
               max(iif(hai.Score = 'Not Available' or hai.Score = '--', null, cast(hai.Score as decimal(10, 3)))),
               min(iif(hai.Score = 'Not Available' or hai.Score = '--', null, cast(hai.Score as decimal(10, 3)))),
               cast(avg(iif(hai.Score = 'Not Available' or hai.Score = '--', null,
                            cast(hai.Score as decimal(10, 3)))) as decimal(10, 3))
        from DataImport.Healthcare_Associated_Infections hai
                 left join (
            select distinct hai.MeasureName,
                            b.AverageScoreBetter,
                            w.AverageScoreWorse,
                            no.AverageScoreNoDifferent,
                            iif(b.AverageScoreBetter is not null and w.AverageScoreWorse is not null,
                                iif(b.AverageScoreBetter > w.AverageScoreWorse,
                                    0, -- Largest Score Is High Rank
                                    1 --Largest Score Is Low Rank
                                    ),
                                iif(b.AverageScoreBetter is null,
                                    iif(no.AverageScoreNoDifferent > w.AverageScoreWorse,
                                        0, -- Largest Score Is High Rank
                                        1 --Largest Score Is Low Rank
                                        ),
                                    iif(w.AverageScoreWorse is null,
                                        iif(b.AverageScoreBetter > no.AverageScoreNoDifferent,
                                            0, -- Largest Score Is High Rank
                                            1 --Largest Score Is Low Rank
                                            ),
                                        2 -- score sort order is unknown
                                        )
                                    )
                                ) as SortOrderID
            from DataImport.Healthcare_Associated_Infections hai

                     left join
                 (
                     select hai.MeasureName,
                            avg(iif(hai.Score = 'Not Available' or hai.Score = '--', null,
                                    cast(hai.Score as decimal(10, 3)))
                                ) as AverageScoreBetter,
                            hai.ComparedToNational
                     from DataImport.Healthcare_Associated_Infections hai
                     where hai.ComparedToNational = 'Better than the National Benchmark'
                        or hai.ComparedToNational = 'Better Than the National Rate'
                        or hai.ComparedToNational = 'Better Than the National Value'
                     group by hai.MeasureName, hai.ComparedToNational
                 ) b on b.MeasureName = hai.MeasureName
                     left join
                 (
                     select hai.MeasureName,
                            avg(iif(hai.Score = 'Not Available' or hai.Score = '--', null,
                                    cast(hai.Score as decimal(10, 3)))
                                ) as AverageScoreWorse,
                            hai.ComparedToNational
                     from DataImport.Healthcare_Associated_Infections hai
                     where hai.ComparedToNational = 'Worse than the National Benchmark'
                        or hai.ComparedToNational = 'Worse Than the National Rate'
                        or hai.ComparedToNational = 'Worse Than the National Value'
                     group by hai.MeasureName, hai.ComparedToNational
                 ) w on w.MeasureName = hai.MeasureName
                     left join
                 (
                     select hai.MeasureName,
                            avg(iif(hai.Score = 'Not Available' or hai.Score = '--', null,
                                    cast(hai.Score as decimal(10, 3)))
                                ) as AverageScoreNoDifferent,
                            hai.ComparedToNational
                     from DataImport.Healthcare_Associated_Infections hai
                     where hai.ComparedToNational = 'No Different than the National Benchmark'
                        or hai.ComparedToNational = 'No Different Than the National Rate'
                        or hai.ComparedToNational = 'No Different Than the National Value'
                     group by hai.MeasureName, hai.ComparedToNational
                 ) no on no.MeasureName = hai.MeasureName
        ) as so on so.MeasureName = hai.MeasureName
        group by hai.MeasureID, hai.MeasureName, so.SortOrderID

        order by MeasureID
    commit; -- if no errors, commit

end try
begin catch
    rollback; -- if errors, rollback
end catch
go;

/*------------------------------------------*/
/*---- Insert to FacilityMeasures table ----*/
/*------------------------------------------*/
-- insert into Measures_CaD table
begin try
    -- begin try
    begin tran
        -- begin transaction


        insert into FacilityMeasures_US.FacilityMeasures(FacilityID,
                                                         MeasureID,
                                                         ComparedToNational,
                                                         Denominator,
                                                         Score,
                                                         LowerEstimate,
                                                         HigherEstimate,
                                                         Footnote,
                                                         StartDate,
                                                         EndDate)
        select distinct f.FacilityID,
                        m.MeasureID,
                        iif(cd.ComparedToNational = 'Not Available', null, cd.ComparedToNational) as ComparedToNational,
                        iif(cd.Denominator = 'Not Available', null,
                            cast(cd.Denominator as decimal(10, 3)))                               as Denominator,
                        iif(cd.Score = 'Not Available', null, cast(cd.Score as decimal(10, 3)))   as Score,
                        iif(cd.LowerEstimate = 'Not Available', null,
                            cast(cd.LowerEstimate as decimal(10, 3)))                             as LowerEstimate,
                        iif(cd.HigherEstimate = 'Not Available', null,
                            cast(cd.HigherEstimate as decimal(10, 3)))                            as HigherEstimate,
                        cd.Footnote,
                        cast(cd.StartDate as date)                                                as StartDate,
                        cast(cd.EndDate as date)                                                  as EndDate
        from DataImport.Complications_and_Deaths cd
                 join FacilityMeasures_US.Facilities f on cd.FacilityID = f.FacilityCode
                 join FacilityMeasures_US.Measures m on cd.MeasureID = MeasureCode
        union
        select distinct f.FacilityID,
                        m.MeasureID,
                        iif(hai.ComparedToNational = 'Not Available', null,
                            hai.ComparedToNational)            as ComparedToNational,
                        null                                   as Denominator,
                        iif(hai.Score = 'Not Available' or hai.Score = '--', null,
                            cast(hai.Score as decimal(10, 3))) as Score,
                        null                                   as LowerEstimate,
                        null                                   as HigherEstimate,
                        hai.Footnote,
                        cast(hai.StartDate as date)            as StartDate,
                        cast(hai.EndDate as date)              as EndDate
        from DataImport.Healthcare_Associated_Infections hai
                 join FacilityMeasures_US.Facilities f on hai.FacilityID = f.FacilityCode
                 join FacilityMeasures_US.Measures m on hai.MeasureID = MeasureCode
    commit; -- if no errors, commit

end try
begin catch
    rollback; -- if errors, rollback
end catch
go;