select dd.FacilityName,
       fm.MeasureName,
       fm.Score
from FacilityMeasures_WI_DW.FactMeasure fm
         left join FacilityMeasures_WI_DW.DimFacility dd on fm.FacilityKey = dd.FacilityKey
-- where dd.FacilityName = 'WAUKESHA MEMORIAL HOSPITAL'
order by dd.FacilityName, fm.MeasureName

select f.FacilityName,
       m.MeasureName,
       fm.Score
from FacilityMeasures_WI.FacilityMeasures fm
         join FacilityMeasures_WI.Facilities f on fm.FacilityID = f.FacilityID
         join FacilityMeasures_WI.Measures m on fm.MeasureID = m.MeasureID
-- where f.FacilityName = 'WAUKESHA MEMORIAL HOSPITAL'
order by f.FacilityName

select f.FacilityName,
       m.MeasureName,
       fm.Score
from FacilityMeasures_US.FacilityMeasures fm
         join FacilityMeasures_US.Facilities f on fm.FacilityID = f.FacilityID
         join FacilityMeasures_US.Measures m on fm.MeasureID = m.MeasureID
-- where f.FacilityName = 'WAUKESHA MEMORIAL HOSPITAL'

select cd.FacilityName,
       cd.MeasureName,
       cd.Score
from DataImport.Complications_and_Deaths cd
where cd.FacilityName = 'MILWAUKEE VA MEDICAL CENTER'
union
select hai.FacilityName,
       hai.MeasureName,
       hai.Score
from DataImport.Healthcare_Associated_Infections hai
where hai.FacilityName = 'MILWAUKEE VA MEDICAL CENTER'