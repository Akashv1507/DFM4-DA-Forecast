create table dfm4_dayahead_demand_forecast
(
id NUMBER GENERATED BY DEFAULT ON NULL AS IDENTITY,
time_stamp date,
entity_tag varchar2(100),
forecasted_demand_value number,
constraints dfm4_unique_forecasted_demand unique(time_stamp,entity_tag),
constraints dfm4_pk_forecasted_demand primary key(id)
)