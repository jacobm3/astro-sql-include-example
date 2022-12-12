SELECT
  w.date_recorded,
  MAX(prcp) AS prcp,
  MAX(tmin) AS tmin,
  MAX(tmax) AS tmax,
  case when MAX(haswx) = True then True else False end AS haswx,
  datediff('day', current_date(), to_date(date_recorded, 'YYYYMMDD')) as diff
FROM (
  SELECT
    wx.date_recorded,
    case when wx.element = 'PRCP' then wx.value/10 end AS prcp,
    case when wx.element = 'TMIN' then wx.value/10 end AS tmin,
    case when wx.element = 'TMAX' then wx.value/10 end AS tmax,
    case when substr(wx.element, 0, 2) = 'WT' then True end AS haswx -- has impactful weather conditions
  FROM
    aws_stg_cosmic_energy.weather_elements AS wx
  WHERE
    id = 'USW00094846' -- CHICAGO OHARE INTL AP
    AND qflag IS NULL 
    and datediff('day', to_date(date_recorded, 'YYYYMMDD'), current_date()) < 15 
) w
GROUP BY
  w.date_recorded
ORDER BY
  w.date_recorded desc
limit 10000