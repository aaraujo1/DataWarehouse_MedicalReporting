/*------------------------------------*/
/*---- Insert to Facilities table ----*/
/*------------------------------------*/

-- insert into Facilities table
begin try
    -- begin try
    begin tran
        -- begin transaction


        insert into FacilityMeasures_WI.Facilities(FacilityCode,
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
        where State = 'WI'
          and CountyName in ('Milwaukee', 'Waukesha', 'Washington')
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
        where State = 'WI'
          and CountyName in ('Milwaukee', 'Waukesha', 'Washington')
    commit; -- if no errors, commit

end try
begin catch
    rollback; -- if errors, rollback
end catch
go;

/*--------------------------------------*/
/*---- Insert to MeasureTypes table ----*/
/*--------------------------------------*/
insert into FacilityMeasures_WI.MeasureTypes (MeasureTypeName)
values ('Complications and Deaths'),
       ('Healthcare Associated Infections');
go;

/*------------------------------------------*/
/*---- Insert to MeasureSortOrder table ----*/
/*------------------------------------------*/
insert into FacilityMeasures_WI.MeasureSortOrder (MeasureSortOrderName)
values ('Unknown'),
       ('Largest Score Is High Rank'),
       ('Largest Score Is Low Rank');
go;

/*----------------------------------*/
/*---- Insert to Measures table ----*/
/*----------------------------------*/

-- insert into Measures table
begin try
    -- begin try
    begin tran
        -- begin transaction


        insert into FacilityMeasures_WI.Measures(MeasureCode,
                                                 MeasureName,
                                                 MeasureTypeID,
                                                 SortOrderID,
                                                 NationalScoreMax,
                                                 NationalScoreMin,
                                                 NationalScoreAverage)
            -- cd
        select distinct cd.MeasureID,
                        cd.MeasureName,
                        1,
                        so.SortOrderID,
                        us.MaxScore,
                        us.MinScore,
                        us.AvgScore
        from DataImport.Complications_and_Deaths cd
                 left join (
            select cd.MeasureID,
                   max(iif(cd.Score = 'Not Available' or cd.Score = '--', null,
                           cast(cd.Score as decimal(10, 3))))                                                      as MaxScore,
                   min(iif(cd.Score = 'Not Available' or cd.Score = '--', null,
                           cast(cd.Score as decimal(10, 3))))                                                      as MinScore,
                   cast(avg(iif(cd.Score = 'Not Available' or cd.Score = '--', null,
                                cast(cd.Score as decimal(10, 3)))) as decimal(10, 3))                              as AvgScore
            from DataImport.Complications_and_Deaths cd
            group by cd.MeasureID
        ) as us on us.MeasureID = cd.MeasureID
                 left join (
            select distinct cd.MeasureName,
                            b.AverageScoreBetter,
                            w.AverageScoreWorse,
                            no.AverageScoreNoDifferent,
                            iif(b.AverageScoreBetter is not null and w.AverageScoreWorse is not null,
                                iif(b.AverageScoreBetter > w.AverageScoreWorse,
                                    2, -- Largest Score Is High Rank
                                    3 --Largest Score Is Low Rank
                                    ),
                                iif(b.AverageScoreBetter is null,
                                    iif(no.AverageScoreNoDifferent > w.AverageScoreWorse,
                                        2, -- Largest Score Is High Rank
                                        3 --Largest Score Is Low Rank
                                        ),
                                    iif(w.AverageScoreWorse is null,
                                        iif(b.AverageScoreBetter > no.AverageScoreNoDifferent,
                                            2, -- Largest Score Is High Rank
                                            3 --Largest Score Is Low Rank
                                            ),
                                        1
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
        where State = 'WI'
          and CountyName in ('Milwaukee', 'Waukesha', 'Washington')


          -- union
        union

        -- hai
        select hai.MeasureID,
               hai.MeasureName,
               2,
               so.SortOrderID,
               us.MaxScore,
               us.MinScore,
               us.AvgScore
        from DataImport.Healthcare_Associated_Infections hai

                 left join (
            select hai.MeasureID,
                   max(iif(hai.Score = 'Not Available' or hai.Score = '--', null,
                           cast(hai.Score as decimal(10, 3))))                                                        as MaxScore,
                   min(iif(hai.Score = 'Not Available' or hai.Score = '--', null,
                           cast(hai.Score as decimal(10, 3))))                                                        as MinScore,
                   cast(avg(iif(hai.Score = 'Not Available' or hai.Score = '--', null,
                                cast(hai.Score as decimal(10, 3)))) as decimal(10, 3))                                as AvgScore
            from DataImport.Healthcare_Associated_Infections hai
            group by hai.MeasureID
        ) as us on us.MeasureID = hai.MeasureID

                 left join (
            select distinct hai.MeasureName,
                            b.AverageScoreBetter,
                            w.AverageScoreWorse,
                            no.AverageScoreNoDifferent,
                            iif(b.AverageScoreBetter is not null and w.AverageScoreWorse is not null,
                                iif(b.AverageScoreBetter > w.AverageScoreWorse,
                                    2, -- Largest Score Is High Rank
                                    3 --Largest Score Is Low Rank
                                    ),
                                iif(b.AverageScoreBetter is null,
                                    iif(no.AverageScoreNoDifferent > w.AverageScoreWorse,
                                        2, -- Largest Score Is High Rank
                                        3 --Largest Score Is Low Rank
                                        ),
                                    iif(w.AverageScoreWorse is null,
                                        iif(b.AverageScoreBetter > no.AverageScoreNoDifferent,
                                            2, -- Largest Score Is High Rank
                                            3 --Largest Score Is Low Rank
                                            ),
                                        1
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
        where State = 'WI'
          and CountyName in ('Milwaukee', 'Waukesha', 'Washington')

        order by MeasureID
    commit; -- if no errors, commit

end try
begin catch
    rollback; -- if errors, rollback
end catch
go;

/*--------------------------------------*/
/*---- Insert to FacilityMeasures table ----*/
/*--------------------------------------*/
-- insert into FacilityMeasures table
begin try
    -- begin try
    begin tran
        -- begin transaction


        insert into FacilityMeasures_WI.FacilityMeasures(FacilityID,
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
                 join FacilityMeasures_WI.Facilities f on cd.FacilityID = f.FacilityCode
                 join FacilityMeasures_WI.Measures m on cd.MeasureID = MeasureCode
        where cd.State = 'WI'
          and cd.CountyName in ('Milwaukee', 'Waukesha', 'Washington')
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
                 join FacilityMeasures_WI.Facilities f on hai.FacilityID = f.FacilityCode
                 join FacilityMeasures_WI.Measures m on hai.MeasureID = MeasureCode
        where hai.State = 'WI'
          and hai.CountyName in ('Milwaukee', 'Waukesha', 'Washington')
    commit; -- if no errors, commit

end try
begin catch
    rollback; -- if errors, rollback
end catch
go;