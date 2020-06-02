/*-------------------------------*/
/*----------- DimDate -----------*/
/*-------------------------------*/


-- populate DimDate beginning of time and end of time
-- beginning of time
if (not exists(select *
               from FacilityMeasures_WI_DW.DimDate
               where DateKey = 19000101))
    begin
        declare @d datetime = '1/1/1900'
-- this is our "beginning of time"
        insert into FacilityMeasures_WI_DW.DimDate
        select cast(convert(char(10), @d, 112) as int), -- key
               @d,                                      -- date
               datepart(d, @d),                         -- day of month
               datepart(m, @d),                         -- month number
               datename(m, @d),                         -- month name
               datepart(yy, @d),                        -- year
               datename(dw, @d),                        -- day of the week name
               datepart(dw, @d),                        -- day of the week number
               convert(bit,
                       case
                           when datepart(dw, @d) in (1, 7)
                               then 1
                           else 0
                           end
                   )
        -- if saturday or sunday, then is weekend
    end
go;

-- end of time insert
if (not exists(select *
               from FacilityMeasures_WI_DW.DimDate
               where DateKey = 99990101))
    begin
        declare @d datetime = '1/1/9999'
-- this is our "end of time"
        insert into FacilityMeasures_WI_DW.DimDate
        select cast(convert(char(10), @d, 112) as int), -- key
               @d,                                      -- date
               datepart(d, @d),                         -- day of month
               datepart(m, @d),                         -- month number
               datename(m, @d),                         -- month name
               datepart(yy, @d),                        -- year
               datename(dw, @d),                        -- day of the week name
               datepart(dw, @d),                        -- day of the week number
               convert(bit,
                       case
                           when datepart(dw, @d) in (1, 7)
                               then 1
                           else 0
                           end
                   )
        -- if saturday or sunday, then is weekend
    end;
go;


-- find earliest date
select min(StartDate), -- 2015-04-01
       min(EndDate)    -- 2018-03-31
from FacilityMeasures_WI.FacilityMeasures;


-- start date will be 2015-1-1
-- end date will be 2030-1-1
-- NOTE: unsure if I should have start date and end date to greater numbers

-- populate DimDate
declare @d datetime = '1/1/2015'
-- could be '1/1/1900'
-- keeping it high for disk space
-- this is our "date zero"
while (not (@d > '1/1/2030'))
    -- could be '1/1/9999'
    -- keeping it low for disk space
    begin
        insert into FacilityMeasures_WI_DW.DimDate
        select cast(convert(char(10), @d, 112) as int), -- key
               @d,                                      -- date
               datepart(d, @d),                         -- day of month
               datepart(m, @d),                         -- month number
               datename(m, @d),                         -- month name
               datepart(yy, @d),                        -- year
               datename(dw, @d),                        -- day of the week name
               datepart(dw, @d),                        -- day of the week number
               convert(bit,
                       case
                           when datepart(dw, @d) in (1, 7)
                               then 1
                           else 0
                           end
                   )
        -- if saturday or sunday, then is weekend

        -- change value of @d
        set @d = dateadd(day, 1, @d)
    end
go;

-- test
select *
from FacilityMeasures_WI_DW.DimDate;


/*-----------------------------------*/
/*----------- DimFacility -----------*/
/*-----------------------------------*/
/*********************************
* Procedure DimFacility_upsert
*
* Author: André Araujo
* Created: 05/23/2020
*
* This procedure populates the DimFacility table.
*
* Change Log
* ---------------------------
* 05/24/2020 - Changed merge to be on FacilityID and FacilityName
  to make sure they are the same as new data
* 06/01/2020 - Merge only on Facility Name in case ID changes
* ***********************************/
create or alter procedure FacilityMeasures_WI_DW.DimFacility_upsert
as
begin
    merge into FacilityMeasures_WI_DW.DimFacility as tgt
    using FacilityMeasures_WI.Facilities as src
    on -- tgt.FacilityID = src.FacilityID and
        tgt.FacilityName = src.FacilityName -- ensures ID and Name are the same
    -- only on name
    when matched then
        update
        set tgt.FacilityID          = src.FacilityID,
            tgt.FacilityAddress     = src.Address,
            tgt.FacilityCity        = src.City,
            tgt.FacilityState       = src.State,
            tgt.FacilityZipCode     = src.ZipCode,
            tgt.FacilityCountyName  = src.CountyName,
            tgt.FacilityPhoneNumber = src.PhoneNumber
    when not matched by target then
        insert (FacilityID,
                FacilityName,
                FacilityAddress,
                FacilityCity,
                FacilityState,
                FacilityZipCode,
                FacilityCountyName,
                FacilityPhoneNumber)
        values (src.FacilityID,
                src.FacilityName,
                src.Address,
                src.City,
                src.State,
                src.ZipCode,
                src.CountyName,
                src.PhoneNumber);
end
    ;
go;

-- test procedure
exec FacilityMeasures_WI_DW.DimFacility_upsert

-- test procedure
select *
from FacilityMeasures_WI_DW.DimFacility;

/*-----------------------------------*/
/*----------- FactMeasure -----------*/
/*-----------------------------------*/
/*********************************
* Procedure FactMeasure_upsert
*
* Author: André Araujo
* Created: 05/23/2020
*
* This procedure populates the FactMeasure table.
*
* Change Log
* ---------------------------
* 05/24/2020 - included merge on FacilityMeasureID
* 05/27/2020 - included MeasureSortOrderNumber for excel rank() function
*
* ***********************************/
create or alter procedure FacilityMeasures_WI_DW.FactMeasure_upsert
as
begin
    merge into FacilityMeasures_WI_DW.FactMeasure as tgt
    using (
        select fm.FacilityMeasureID,
               fm.MeasureID,
               FacilityKey  = df.FacilityKey,
               fm.FacilityID,
               m.MeasureName,
               fm.ComparedToNational,
               fm.Score,
               m.NationalScoreMax,
               m.NationalScoreMin,
               m.NationalScoreAverage,
               mso.MeasureSortOrderName,
               mso.MeasureSortOrderID,
               mt.MeasureTypeName,
               fm.StartDate,
               fm.EndDate,
               StartDateKey = ddStart.DateKey,
               EndDateKey   = ddEnd.DateKey
        from FacilityMeasures_WI.FacilityMeasures fm
                 join FacilityMeasures_WI.Measures m on fm.MeasureID = m.MeasureID
                 join FacilityMeasures_WI.MeasureTypes mt on m.MeasureTypeID = mt.MeasureTypeID
                 join FacilityMeasures_WI.MeasureSortOrder mso on m.SortOrderID = mso.MeasureSortOrderID
                 join FacilityMeasures_WI_DW.DimFacility df on fm.FacilityID = df.FacilityID
                 join FacilityMeasures_WI_DW.DimDate ddStart on cast(fm.StartDate as date) = ddStart.Date
                 join FacilityMeasures_WI_DW.DimDate ddEnd on cast(fm.EndDate as date) = ddEnd.Date
        -- order by fm.FacilityMeasureID
    ) as src
    on tgt.FacilityID = src.FacilityID
        and tgt.MeasureID = src.MeasureID
        and tgt.FacilityMeasureID = src.FacilityMeasureID -- all 3 IDs must be the same to update
    when matched then
        update
        set tgt.ScoreComparedToNational = src.ComparedToNational,
            tgt.Score                   = src.Score,
            tgt.NationalScoreHigh       = src.NationalScoreMax,
            tgt.NationalScoreLow        = src.NationalScoreMin,
            tgt.NationalScoreAverage    = src.NationalScoreAverage,
            tgt.StartDate               = src.StartDate,
            tgt.EndDate                 = src.EndDate,
            tgt.StartDateKey            = src.StartDateKey,
            tgt.EndDateKey              = src.EndDateKey
    when not matched by target then
        insert (MeasureID,
                FacilityKey,
                FacilityID,
                FacilityMeasureID,
                MeasureName,
                ScoreComparedToNational,
                Score,
                NationalScoreHigh,
                NationalScoreLow,
                NationalScoreAverage,
                MeasureSortOrderName,
                MeasureSortOrderNumber,
                MeasureTypeName,
                StartDate,
                EndDate,
                StartDateKey,
                EndDateKey)
        values (src.MeasureID,
                src.FacilityKey,
                src.FacilityID,
                src.FacilityMeasureID,
                src.MeasureName,
                src.ComparedToNational,
                src.Score,
                src.NationalScoreMax,
                src.NationalScoreMin,
                src.NationalScoreAverage,
                src.MeasureSortOrderName,
                src.MeasureSortOrderID,
                src.MeasureTypeName,
                src.StartDate,
                src.EndDate,
                src.StartDateKey,
                src.EndDateKey)
        ;

end
    ;
go;

-- test procedure
exec FacilityMeasures_WI_DW.FactMeasure_upsert

-- test procedure
select *
from FacilityMeasures_WI_DW.FactMeasure fm
         join FacilityMeasures_WI_DW.DimFacility df on fm.FacilityKey = df.FacilityKey
         join FacilityMeasures_WI_DW.DimDate ddS on fm.StartDateKey = ddS.DateKey
         join FacilityMeasures_WI_DW.DimDate ddE on fm.EndDateKey = ddE.DateKey

/*-----------------------------------*/
/*----------- ETL Control -----------*/
/*-----------------------------------*/
/*********************************
* Procedure DW_ETL_Control
*
* Author: André Araujo
* Created: 05/23/2020
*
* This procedure populates the FacilityMeasures_WI_DW Data Warehouse.
*
* Change Log
* ---------------------------
* 06/01/2020 - included merge on FacilityMeasureID
*
* ***********************************/
create or alter procedure FacilityMeasures_WI_DW.DW_ETL_Control
as
begin

    begin try
        begin tran
            -- merge FacilityMeasures_US
            exec FacilityMeasures_US.Facilities_Upsert
            exec FacilityMeasures_US.Measures_Upsert
            exec FacilityMeasures_US.FacilityMeasures_Upsert

            -- merge FacilityMeasures_WI
            exec FacilityMeasures_WI.Facilities_Upsert
            exec FacilityMeasures_WI.Measures_Upsert
            exec FacilityMeasures_WI.FacilityMeasures_Upsert

            -- merge Data Warehouse
            exec FacilityMeasures_WI_DW.DimFacility_upsert
            exec FacilityMeasures_WI_DW.FactMeasure_upsert
        commit;
    end try
    begin catch
        rollback;
    end catch
end;
go;

-- test procedure
exec FacilityMeasures_WI_DW.DW_ETL_Control
-- no duplicates created
