/*********************************
* Procedure Drop_and_Create
*
* Author: André Araujo
* Created: 06/201/2020
*
* This procedure drops the tables from DataImport
* and re-creates them
*
* ***********************************/
create or alter procedure DataImport.Drop_and_Create
as
begin

    -- should do backup first

    -- drop Complications and Deaths table
    drop table DataImport.Complications_and_Deaths

    -- drop Healthcare Associated Infections table
    drop table DataImport.Healthcare_Associated_Infections

    -- table for to bulk insert Complications and Deaths table
    create table DataImport.Complications_and_Deaths
    (
        FacilityID         varchar(10),
        FacilityName       varchar(100),
        Address            varchar(100),
        City               varchar(50),
        State              char(2),
        ZipCode            varchar(5),
        CountyName         varchar(50),
        PhoneNumber        varchar(20),
        MeasureID          varchar(50),
        MeasureName        varchar(100),
        ComparedToNational varchar(50),
        Denominator        varchar(20),
        Score              varchar(20), -- some scores are "Not available"
        LowerEstimate      varchar(20),
        HigherEstimate     varchar(20),
        Footnote           varchar(10), -- some footnotes have commas
        StartDate          varchar(10),
        EndDate            varchar(10)
    )

    -- table for to bulk insert Healthcare Associated Infections table
    create table DataImport.Healthcare_Associated_Infections
    (
        FacilityID         varchar(10),
        FacilityName       varchar(100),
        Address            varchar(100),
        City               varchar(50),
        State              char(2),
        ZipCode            varchar(5),
        CountyName         varchar(50),
        PhoneNumber        varchar(20),
        MeasureID          varchar(50),
        MeasureName        varchar(100),
        ComparedToNational varchar(50),
        Score              varchar(20), -- some scores are "Not available"
        Footnote           varchar(10),
        StartDate          varchar(10),-- some footnotes have commas
        EndDate            varchar(10)
    )

    -- afterwards, data import should be done

end

go;

--test
exec DataImport.Drop_and_Create


/*********************************
* Procedure Facilities_Upsert
*
* Author: André Araujo
* Created: 06/1/2020
*
* This procedure populates the Facilities table
*
* ***********************************/
create or alter procedure FacilityMeasures_US.Facilities_Upsert
as
begin
    begin try
        begin tran
            merge into FacilityMeasures_US.Facilities as tgt
            using
                (
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
                ) as src
            on tgt.FacilityName = src.FacilityName
            when matched then
                update
                set tgt.FacilityCode = src.FacilityID,
                    tgt.Address      = src.Address,
                    tgt.City         = src.City,
                    tgt.State        = src.State,
                    tgt.ZipCode      = src.ZipCode,
                    tgt.CountyName   = src.CountyName,
                    tgt.PhoneNumber  = src.PhoneNumber
            when not matched by target then
                insert (FacilityCode,
                        FacilityName,
                        Address,
                        City,
                        State,
                        ZipCode,
                        CountyName,
                        PhoneNumber)
                values (src.FacilityID,
                        src.FacilityName,
                        src.Address,
                        src.City,
                        src.State,
                        src.ZipCode,
                        src.CountyName,
                        src.PhoneNumber);
        commit;
    end try
    begin catch
        rollback;
    end catch
end
go;

exec FacilityMeasures_US.Facilities_Upsert


/*********************************
* Procedure Measures_Upsert
*
* Author: André Araujo
* Created: 06/1/2020
*
* This procedure populates the Measures table
*
* ***********************************/
create or alter procedure FacilityMeasures_US.Measures_Upsert
as
begin
    begin try
        begin tran
            merge into FacilityMeasures_US.Measures as tgt
            using
                (
                    select cd.MeasureID,
                           cd.MeasureName,
                           1                                                                  as MeasureTypeID,
                           so.SortOrderID, -- change
                           max(iif(cd.Score = 'Not Available' or cd.Score = '--', null,
                                   cast(cd.Score as decimal(10, 3))))                         as MaxScore,
                           min(iif(cd.Score = 'Not Available' or cd.Score = '--', null,
                                   cast(cd.Score as decimal(10, 3))))                         as MinScore,
                           cast(avg(iif(cd.Score = 'Not Available' or cd.Score = '--', null,
                                        cast(cd.Score as decimal(10, 3)))) as decimal(10, 3)) as AvgScore
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
                           2                                                                   as MeasureTypeID,
                           so.SortOrderID, -- change
                           max(iif(hai.Score = 'Not Available' or hai.Score = '--', null,
                                   cast(hai.Score as decimal(10, 3))))                         as MaxScore,
                           min(iif(hai.Score = 'Not Available' or hai.Score = '--', null,
                                   cast(hai.Score as decimal(10, 3))))                         as MinScore,
                           cast(avg(iif(hai.Score = 'Not Available' or hai.Score = '--', null,
                                        cast(hai.Score as decimal(10, 3)))) as decimal(10, 3)) as AvgScore
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
                ) as src
            on tgt.MeasureName = src.MeasureName
            when matched then
                update
                set tgt.MeasureCode          = src.MeasureID,
                    tgt.MeasureTypeID        = src.MeasureTypeID,
                    tgt.SortOrderID          = src.SortOrderID,
                    tgt.NationalScoreMax     = src.MaxScore,
                    tgt.NationalScoreMin     = src.MinScore,
                    tgt.NationalScoreAverage = AvgScore
            when not matched by target then
                insert (MeasureCode,
                        MeasureName,
                        MeasureTypeID,
                        SortOrderID,
                        NationalScoreMax,
                        NationalScoreMin,
                        NationalScoreAverage)
                values (src.MeasureID,
                        src.MeasureName,
                        src.MeasureTypeID,
                        src.SortOrderID,
                        src.MaxScore,
                        src.MinScore,
                        src.AvgScore);
        commit;
    end try
    begin catch
        rollback;
    end catch
end
go;

--test
exec FacilityMeasures_US.Measures_Upsert


/*********************************
* Procedure FacilityMeasures_Upsert
*
* Author: André Araujo
* Created: 06/1/2020
*
* This procedure populates the FacilityMeasures table
*
* ***********************************/
create or alter procedure FacilityMeasures_US.FacilityMeasures_Upsert
as
begin
    begin try
        begin tran
            merge into FacilityMeasures_US.FacilityMeasures as tgt
            using (
                select distinct f.FacilityID,
                                m.MeasureID,
                                iif(cd.ComparedToNational = 'Not Available', null,
                                    cd.ComparedToNational)                                              as ComparedToNational,
                                iif(cd.Denominator = 'Not Available', null,
                                    cast(cd.Denominator as decimal(10, 3)))                             as Denominator,
                                iif(cd.Score = 'Not Available', null, cast(cd.Score as decimal(10, 3))) as Score,
                                iif(cd.LowerEstimate = 'Not Available', null,
                                    cast(cd.LowerEstimate as decimal(10, 3)))                           as LowerEstimate,
                                iif(cd.HigherEstimate = 'Not Available', null,
                                    cast(cd.HigherEstimate as decimal(10, 3)))                          as HigherEstimate,
                                cd.Footnote,
                                cast(cd.StartDate as date)                                              as StartDate,
                                cast(cd.EndDate as date)                                                as EndDate
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
            ) as src
            on tgt.MeasureID = src.MeasureID and
               tgt.FacilityID = src.FacilityID
            when matched then
                update
                set tgt.ComparedToNational = src.ComparedToNational,
                    tgt.Denominator        = src.Denominator,
                    tgt.Score              = src.Score,
                    tgt.LowerEstimate      = src.LowerEstimate,
                    tgt.HigherEstimate     = src.HigherEstimate,
                    tgt.Footnote           = src.Footnote,
                    tgt.StartDate          = src.StartDate,
                    tgt.EndDate            = src.EndDate
            when not matched by target then
                insert (FacilityID,
                        MeasureID,
                        ComparedToNational,
                        Denominator,
                        Score,
                        LowerEstimate,
                        HigherEstimate,
                        Footnote,
                        StartDate,
                        EndDate)
                values (src.FacilityID,
                        src.MeasureID,
                        src.ComparedToNational,
                        src.Denominator,
                        src.Score,
                        src.LowerEstimate,
                        src.HigherEstimate,
                        src.Footnote,
                        src.StartDate,
                        src.EndDate);
        commit;
    end try
    begin catch
        rollback;
    end catch
end
go;

--test
exec FacilityMeasures_US.FacilityMeasures_Upsert


/*------- WI --------*/


/*********************************
* Procedure Facilities_Upsert
*
* Author: André Araujo
* Created: 06/1/2020
*
* This procedure populates the Facilities table
*
* ***********************************/
create or alter procedure FacilityMeasures_WI.Facilities_Upsert
as
begin
    begin try
        begin tran
            merge into FacilityMeasures_WI.Facilities as tgt
            using
                (
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
                ) as src
            on tgt.FacilityName = src.FacilityName
            when matched then
                update
                set tgt.FacilityCode = src.FacilityID,
                    tgt.Address      = src.Address,
                    tgt.City         = src.City,
                    tgt.State        = src.State,
                    tgt.ZipCode      = src.ZipCode,
                    tgt.CountyName   = src.CountyName,
                    tgt.PhoneNumber  = src.PhoneNumber
            when not matched by target then
                insert (FacilityCode,
                        FacilityName,
                        Address,
                        City,
                        State,
                        ZipCode,
                        CountyName,
                        PhoneNumber)
                values (src.FacilityID,
                        src.FacilityName,
                        src.Address,
                        src.City,
                        src.State,
                        src.ZipCode,
                        src.CountyName,
                        src.PhoneNumber);
        commit;
    end try
    begin catch
        rollback;
    end catch
end
go;

exec FacilityMeasures_WI.Facilities_Upsert


/*********************************
* Procedure Measures_Upsert
*
* Author: André Araujo
* Created: 06/201/2020
*
* This procedure populates the Measures table
*
* Note:
* Currently not working. Could be built from
  FacilityMeasures_US.Measures with filter instead
*
* ***********************************/
create or alter procedure FacilityMeasures_WI.Measures_Upsert
as
begin
    begin try
        begin tran
            merge into FacilityMeasures_WI.Measures as tgt
            using
                (
                    select distinct cd.MeasureID,
                                    cd.MeasureName,
                                    1 as MeasureTypeID,
                                    so.SortOrderID,
                                    us.MaxScore,
                                    us.MinScore,
                                    us.AvgScore
                    from DataImport.Complications_and_Deaths cd
                             left join (
                        select cd.MeasureID,
                               max(iif(cd.Score = 'Not Available' or cd.Score = '--', null,
                                       cast(cd.Score as decimal(10, 3))))                         as MaxScore,
                               min(iif(cd.Score = 'Not Available' or cd.Score = '--', null,
                                       cast(cd.Score as decimal(10, 3))))                         as MinScore,
                               cast(avg(iif(cd.Score = 'Not Available' or cd.Score = '--', null,
                                            cast(cd.Score as decimal(10, 3)))) as decimal(10, 3)) as AvgScore
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
                                                0, -- Largest Score Is High Rank
                                                1 -- Largest Score Is Low Rank
                                                ),
                                            iif(b.AverageScoreBetter is null,
                                                iif(no.AverageScoreNoDifferent > w.AverageScoreWorse,
                                                    0, -- Largest Score Is High Rank
                                                    1 -- Largest Score Is Low Rank
                                                    ),
                                                iif(w.AverageScoreWorse is null,
                                                    iif(b.AverageScoreBetter > no.AverageScoreNoDifferent,
                                                        0, -- Largest Score Is High Rank
                                                        1 -- Largest Score Is Low Rank
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
                    where State = 'WI'
                      and CountyName in ('Milwaukee', 'Waukesha', 'Washington') -- remove if all of WI is wanted


                      -- union
                    union

                    -- hai
                    select hai.MeasureID,
                           hai.MeasureName,
                           2 as MeasureTypeID,
                           so.SortOrderID,
                           us.MaxScore,
                           us.MinScore,
                           us.AvgScore
                    from DataImport.Healthcare_Associated_Infections hai

                             left join (
                        select hai.MeasureID,
                               max(iif(hai.Score = 'Not Available' or hai.Score = '--', null,
                                       cast(hai.Score as decimal(10, 3))))                         as MaxScore,
                               min(iif(hai.Score = 'Not Available' or hai.Score = '--', null,
                                       cast(hai.Score as decimal(10, 3))))                         as MinScore,
                               cast(avg(iif(hai.Score = 'Not Available' or hai.Score = '--', null,
                                            cast(hai.Score as decimal(10, 3)))) as decimal(10, 3)) as AvgScore
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
                    where State = 'WI'
                      and CountyName in ('Milwaukee', 'Waukesha', 'Washington')
                ) as src
            on tgt.MeasureName = src.MeasureName
            when matched then
                update
                set tgt.MeasureCode          = src.MeasureID,
                    tgt.MeasureTypeID        = src.MeasureTypeID,
                    tgt.SortOrderID          = src.SortOrderID,
                    tgt.NationalScoreMax     = src.MaxScore,
                    tgt.NationalScoreMin     = src.MinScore,
                    tgt.NationalScoreAverage = AvgScore
            when not matched by target then
                insert (MeasureCode,
                        MeasureName,
                        MeasureTypeID,
                        SortOrderID,
                        NationalScoreMax,
                        NationalScoreMin,
                        NationalScoreAverage)
                values (src.MeasureID,
                        src.MeasureName,
                        src.MeasureTypeID,
                        src.SortOrderID,
                        src.MaxScore,
                        src.MinScore,
                        src.AvgScore);
        commit;
    end try
    begin catch
        rollback;
    end catch
end
go;

--test
exec FacilityMeasures_WI.Measures_Upsert


/*********************************
* Procedure FacilityMeasures_Upsert
*
* Author: André Araujo
* Created: 06/201/2020
*
* This procedure populates the FacilityMeasures table
*
* ***********************************/
create or alter procedure FacilityMeasures_WI.FacilityMeasures_Upsert
as
begin
    begin try
        begin tran
            merge into FacilityMeasures_WI.FacilityMeasures as tgt
            using (
                select distinct f.FacilityID,
                                m.MeasureID,
                                iif(cd.ComparedToNational = 'Not Available', null,
                                    cd.ComparedToNational)                                              as ComparedToNational,
                                iif(cd.Denominator = 'Not Available', null,
                                    cast(cd.Denominator as decimal(10, 3)))                             as Denominator,
                                iif(cd.Score = 'Not Available', null, cast(cd.Score as decimal(10, 3))) as Score,
                                iif(cd.LowerEstimate = 'Not Available', null,
                                    cast(cd.LowerEstimate as decimal(10, 3)))                           as LowerEstimate,
                                iif(cd.HigherEstimate = 'Not Available', null,
                                    cast(cd.HigherEstimate as decimal(10, 3)))                          as HigherEstimate,
                                cd.Footnote,
                                cast(cd.StartDate as date)                                              as StartDate,
                                cast(cd.EndDate as date)                                                as EndDate
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
            ) as src
            on tgt.MeasureID = src.MeasureID and
               tgt.FacilityID = src.FacilityID
            when matched then
                update
                set tgt.ComparedToNational = src.ComparedToNational,
                    tgt.Denominator        = src.Denominator,
                    tgt.Score              = src.Score,
                    tgt.LowerEstimate      = src.LowerEstimate,
                    tgt.HigherEstimate     = src.HigherEstimate,
                    tgt.Footnote           = src.Footnote,
                    tgt.StartDate          = src.StartDate,
                    tgt.EndDate            = src.EndDate
            when not matched by target then
                insert (FacilityID,
                        MeasureID,
                        ComparedToNational,
                        Denominator,
                        Score,
                        LowerEstimate,
                        HigherEstimate,
                        Footnote,
                        StartDate,
                        EndDate)
                values (src.FacilityID,
                        src.MeasureID,
                        src.ComparedToNational,
                        src.Denominator,
                        src.Score,
                        src.LowerEstimate,
                        src.HigherEstimate,
                        src.Footnote,
                        src.StartDate,
                        src.EndDate);
        commit;
    end try
    begin catch
        rollback;
    end catch
end
go;

--test
exec FacilityMeasures_US.FacilityMeasures_Upsert



--