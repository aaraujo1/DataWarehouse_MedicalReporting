/*------------------------*/
/*---- Create Schemas ----*/
/*------------------------*/

-- create schema to data import data
create schema DataImport
go;


/*--------------------------------------*/
/*---- Create Tables for BulkInsert ----*/
/*--------------------------------------*/

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
go;

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
    Footnote           varchar(10), -- some footnotes have commas
    StartDate          varchar(10),
    EndDate            varchar(10)
)
go;

