import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import globdata as gd

dloader = gd.DataLoader()

files_list_3 = ['MERRA2_400.tavg1_2d_flx_Nx.20220428.nc4',
              'MERRA2_400.tavg1_2d_flx_Nx.20220429.nc4',
              'MERRA2_400.tavg1_2d_flx_Nx.20220430.nc4']

files_list_1 = ['MERRA2_400.tavg1_2d_flx_Nx.20220430.nc4']

vars_interest_list = ['evaporation_from_turbulence',
                      'areal_fraction_of_anvil_showers',
                      'areal_fraction_of_convective_showers',
                      'areal_fraction_of_nonanvil_large_scale_showers',
                      'sensible_heat_flux_from_turbulence', 'convective_precipitation',
                      'total_precipitation', 'surface_air_temperature']

df_time_day = dloader.time_series(files_list_1, 32.5, 35, vars_interest_list, cache = True, as_numpy = False)

sns.pairplot(df_time_day, kind = 'reg')

# calc correlation matrices
corr_pearson = df_time_day[vars_interest_list].corr(method='pearson')
corr_spearman = df_time_day[vars_interest_list].corr(method='spearman')

print(corr_pearson)
print(corr_spearman)



