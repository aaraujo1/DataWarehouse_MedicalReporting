/*------------------------*/
/*---- Create Schemas ----*/
/*------------------------*/

-- create schema for data warehouse
-- create schema FacilityMeasures_WI_DW
-- go;

/*------------------------------------------*/
/*---- Create Tables for Data Warehouse ----*/
/*------------------------------------------*/
-- for facilities
create table FacilityMeasures_WI_DW.[DimFacility]
(
    [FacilityKey]         int identity (1,1),
    [FacilityID]          int          not null,
    [FacilityName]        varchar(100) not null,
    [FacilityAddress]     varchar(100) null,
    [FacilityCity]        varchar(50)  null,
    [FacilityState]       char(2)      not null,
    [FacilityZipCode]     varchar(5)   null,
    [FacilityCountyName]  varchar(50)  not null,
    [FacilityPhoneNumber] varchar(20)  null,
    constraint pk_DimFacility primary key (FacilityKey)
);
go;

-- for dates
create table FacilityMeasures_WI_DW.[DimDate]
(
    [DateKey]         int         not null,
    [Date]            date        not null,
    [DayOfMonth]      tinyint     not null,
    [MonthNumber]     tinyint     not null,
    [MonthName]       varchar(10) not null,
    [Year]            smallint    not null,
    [DayOfWeekName]   varchar(10) not null,
    [DayOfWeekNumber] tinyint     not null,
    [IsWeekend]       bit         not null,
    constraint pk_DimDate primary key ([DateKey])
);
go;

create table FacilityMeasures_WI_DW.[FactMeasure]
(
    [MeasureKey]              int identity (1,1),
    [MeasureID]               smallint       not null,
    [FacilityKey]             int            not null,
    [FacilityID]              int            not null,
    [FacilityMeasureID]       int            not null,
    [MeasureName]             varchar(100)   not null,
--     [MeasureCode]             varchar(50)    not null,
    [ScoreComparedToNational] varchar(100)   null,
--     [Denominator]             decimal(10, 3) null,
    [Score]                   decimal(10, 3) null,
--     [LowerEstimate]           decimal(10, 3) null,
--     [HighEstimate]            decimal(10, 3) null,
--     [Footnote]                varchar(10)    null,
    [NationalScoreHigh]       decimal(10, 3) not null,
    [NationalScoreLow]        decimal(10, 3) not null,
    [NationalScoreAverage]    decimal(10, 3) not null,
    [MeasureSortOrderName]    varchar(100)   not null,
    [MeasureSortOrderNumber]  tinyint        not null, -- to help with excel rank() function
    [MeasureTypeName]         varchar(100)   not null,
    [StartDate]               date           not null,
    [EndDate]                 date           not null,
    [StartDateKey]            int            not null,
    [EndDateKey]              int            not null,
    constraint pk_FactMeasure primary key (MeasureKey),
    constraint fk_FactMeasure_FacilityKey foreign key (FacilityKey) references FacilityMeasures_WI_DW.DimFacility (FacilityKey),
    constraint fk_FactMeasure_StartDateKey foreign key (StartDateKey) references FacilityMeasures_WI_DW.DimDate (DateKey),
    constraint fk_FactMeasure_EndDateKey foreign key (EndDateKey) references FacilityMeasures_WI_DW.DimDate (DateKey)
);
go;