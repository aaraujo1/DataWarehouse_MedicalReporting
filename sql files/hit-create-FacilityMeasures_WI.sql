/*-----------------------*/
/*---- Create Schema ----*/
/*-----------------------*/

-- create schema for staging area
-- create schema FacilityMeasures_WI
-- go;

/*----------------------------------------*/
/*---- Create Tables for WI Staging Area ----*/
/*----------------------------------------*/

-- table for facilities
create table FacilityMeasures_WI.[Facilities]
(
    [FacilityID]   int identity (1,1),
    [FacilityCode] varchar(10),
    [FacilityName] varchar(100) not null,
    [Address]      varchar(100),
    [City]         varchar(50),
    [State]        char(2)      not null,
    [ZipCode]      varchar(5),
    [CountyName]   varchar(50)  not null,
    [PhoneNumber]  varchar(20),
    constraint pk_Facilities primary key ([FacilityID])
)
go;

-- table for measure type
create table FacilityMeasures_WI.[MeasureTypes]
(
    [MeasureTypeID]   tinyint identity (1,1),
    [MeasureTypeName] varchar(100) not null,
    constraint pk_MeasureTypes primary key ([MeasureTypeID])
)
go;

-- table for measure sort order
create table FacilityMeasures_WI.[MeasureSortOrder]
(
    [MeasureSortOrderID]   tinyint identity (1,1),
    [MeasureSortOrderName] varchar(100) not null,
    constraint pk_MeasureSortOrder primary key ([MeasureSortOrderID])
);
go;


-- table for measures
create table FacilityMeasures_WI.[Measures]
(
    [MeasureID]            smallint identity (1,1),
    [MeasureCode]          varchar(50)    not null,
    [MeasureName]          varchar(100)   not null,
    [MeasureTypeID]        tinyint        not null,
    [SortOrderID]          tinyint        not null,
    [NationalScoreMax]     decimal(10, 3) not null,
    [NationalScoreMin]     decimal(10, 3) not null,
    [NationalScoreAverage] decimal(10, 3) not null,
    constraint pk_Measures primary key ([MeasureID]),
    constraint fk_Measures_MeasureTypeID foreign key (MeasureTypeID)
        references FacilityMeasures_WI.[MeasureTypes] (MeasureTypeID)
)
go;

-- table for facility measures
create table FacilityMeasures_WI.[FacilityMeasures]
(
    [FacilityMeasureID]  int identity (1,1),
    [FacilityID]         int      not null,
    [MeasureID]          smallint not null,
    [ComparedToNational] varchar(100),
    [Denominator]        decimal(10, 3),
    [Score]              decimal(10, 3),
    [LowerEstimate]      decimal(10, 3),
    [HigherEstimate]     decimal(10, 3),
    [Footnote]           varchar(10),
    [StartDate]          date     not null,
    [EndDate]            date     not null,
    constraint pk_FacilityMeasures primary key ([FacilityMeasureID]),
    constraint fk_FacilityMeasures_FacilityID foreign key (FacilityID)
        references FacilityMeasures_WI.[Facilities] (FacilityID),
    constraint fk_FacilityMeasures_MeasureID foreign key (MeasureID)
        references FacilityMeasures_WI.[Measures] (MeasureID),
)
