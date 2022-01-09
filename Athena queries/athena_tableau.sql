select CV.*,ORI.latitude AS origin_state_latitude, ORI.longitude AS origin_state_longitude,
DT.latitude AS dest_state_latitude, DT.longitude AS dest_state_longitude
from 
(
select * from
"cmpt_732_project"."covid_aviation"
where "$PATH" like '%c000%') AS CV
JOIN 
(
select latitude, longitude, origin_state
from "cmpt_732_project"."origin_states"
where "$PATH" like '%origin%') AS ORI
ON CV.origin_state_abr = ORI.origin_state
JOIN 
(
select latitude, longitude, dest_state
from "cmpt_732_project"."dest_states"
where "$PATH" like '%dest%') AS DT
ON CV.dest_state_abr = DT.dest_state