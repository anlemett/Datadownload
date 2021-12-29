
year = '2020'
airport_icao = 'ESSA'
#airport_icao = 'ESGG'


import cdsapi

if airport_icao == 'ESSA':
    TMA_area = '61/17/59/19'
elif airport_icao == 'ESGG':
    TMA_area = '57/11/59/13'

c = cdsapi.Client()

c.retrieve(
    'reanalysis-era5-pressure-levels',
    {
        'product_type': 'reanalysis',
        'format': 'netcdf',
        'variable': [
            'u_component_of_wind', 'v_component_of_wind',
        ],
        'pressure_level': [
            '1', '50', '100',
            '200', '300', '400',
            '500', '600', '700',
            '800', '900', '1000',
        ],
        'year': year,
        'month': [
            '01', '02', '03', '04', '05', '06'
        ],
        'day': [
            '01', '02', '03',
            '04', '05', '06',
            '07', '08', '09',
            '10', '11', '12',
            '13', '14', '15',
            '16', '17', '18',
            '19', '20', '21',
            '22', '23', '24',
            '25', '26', '27',
            '28', '29', '30',
            '31',
        ],
        'time': [
            '00:00', '01:00', '02:00',
            '03:00', '04:00', '05:00',
            '06:00', '07:00', '08:00',
            '09:00', '10:00', '11:00',
            '12:00', '13:00', '14:00',
            '15:00', '16:00', '17:00',
            '18:00', '19:00', '20:00',
            '21:00', '22:00', '23:00',
        ],
        'area': TMA_area,
    },
    'data/' + airport_icao + '/' + airport_icao + '_'  + year + '_01_06_reanalysis_pl.nc')
