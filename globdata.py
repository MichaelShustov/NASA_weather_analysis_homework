# This module contains classes to load and pre-process datasets

import netCDF4 as nc
import pandas as pd


class DataSet:
    """ Class for the nc4 dataset"""

    def __init__(self, filename: str):
        """ Loads the data file, creates dataframe and some descriptive attributes"""

        # open data file
        try:
            ds = nc.Dataset(filename)
        except IOError:
            print('File was not found or other IO error')
            raise NameError('IOError')

        # read dimensions and create a dimensions dict
        self._dim_dict = dict()
        for dim in ds.dimensions.values():
            self._dim_dict[dim.name] = dim.size

        # read names of variables into the dataset and create the variables dict {long_name : short_name}
        self._var_dict = dict()
        for var in ds.variables.values():
            self._var_dict[var.long_name] = var.name

        self._convert2dataframe(ds)
        ds.close()
        # create set of available coordinates
        self._create_coords()

    def _convert2dataframe(self, ds: nc.Dataset):
        """ Converts dataset nc4 to a pd.dataframe

        :param ds: dataset for convertation
        """

        # create a multiindex
        lon_array = ds['lon'][:].data
        lat_array = ds['lat'][:].data
        time_array = ds['time'][:].data

        multi_index = pd.MultiIndex.from_product([time_array, lat_array, lon_array],
                                                 names=['time', 'lat', 'lon'])


        self.df = pd.DataFrame(index=multi_index)
        for k in self._var_dict.keys():
            if k not in {'longitude', 'latitude', 'time'}:
                fl = ds[self._var_dict[k]][:].data.flatten()
                s = pd.Series(fl, index=multi_index)
                self.df[k] = s

        self.df.reset_index(inplace=True)

    def _create_coords(self):
        """ Creates sets of available coordinate values"""

        dfl = self.df
        self._lat_set = set(dfl.loc[dfl.loc[:, 'lat'] <= 90].loc[dfl.loc[:, 'lat'] >= -90]['lat'].tolist())
        self._lon_set = set(dfl.loc[dfl.loc[:, 'lon'] <= 180].loc[dfl.loc[:, 'lon'] >= -180]['lon'].tolist())

    def get_dim_dict(self):
        """ Returns dimensions of the initial dataset"""
        return self._dim_dict

    def get_var_dict(self):
        """ Returns variables names as a dictionary"""
        return self._var_dict

    def get_coord_set(self):
        """ Returns sets of available coordinates (lat, lon)"""
        return self._lat_set, self._lon_set

    def coord_timeseries(self, lat: float, lon: float, var_long: list) -> pd.DataFrame:
        """ Returns time series dataframe at a given coordinate

        :param lat: latitude
        :param lon: longitude
        :param var_long: list of long names of the variables
        :return: timeseries as pd.DataFrame
        """

        if (lon in self._lon_set) and (lat in self._lat_set):
            dfl = self.df

            res = dfl.loc[dfl.loc[:, 'lat'] == lat].loc[dfl.loc[:, 'lon'] == lon][[*['time'], *var_long]]
            return res
        else:
            print('incorrect coordinates')
            return pd.DataFrame()

    def time_slice(self, time: int, var_long: str) -> pd.DataFrame:
        """ Returns dataframe slice for a given time in multiple coordinates

        :param time: time
        :param var_long: long name of the variable
        :return: time slice as dataframe (lat,lon,value)
        """

        dfl = self.df
        res = dfl.loc[dfl.loc[:, 'time'] == time][['lat', 'lon', var_long]]
        return res

    def release(self):
        """ Release memory for the dataframe """
        self.df = None


class DataLoader:
    """ Multiple datasets preprocessing"""

    def __init__(self):
        """ Constructor"""

        # cache dictionary for datasets {filename : dataframe}
        self.ds_dict = dict()

    def _load_dataset(self, file_name: str):
        """ Creates DataSet object, loads data from disc

        :param file_name: name of the nc4 file to load
        """

        try:
            ds = DataSet(file_name)
            # create cache dictionary for datasets
            self.ds_dict[file_name] = ds
        except:
            print('Dataset was not opened')

    def time_series(self, files_list: list,
                    lat: float, lon: float,
                    var_long: list, cache=True,
                    as_numpy=False):
        """ Returns a long time series as DataFrame or np.array for a variable from different files

        :param files_list: list of files (.nc4) with datasets to process
        :param lat: latitude
        :param lon: longitude
        :param var_long: list of long names of the variables
        :param cache: flag to cache the dataset
        :param as_numpy: flag to return a np.array instead pd.DataFrame
        """

        res_df = pd.DataFrame()

        for current_file in files_list:

            # load dataset if not in cache
            if current_file not in self.ds_dict.keys():
                self._load_dataset(current_file)

            curr_df = self.ds_dict[current_file].coord_timeseries(lat, lon, var_long)
            res_df = pd.concat([res_df, curr_df])

            # release memory occupied with dataset, if not cashed
            if not cache:
                del self.ds_dict[current_file]

        if as_numpy:
            return res_df.to_numpy()
        else:
            return res_df

    def time_slice_daily(self, files_list: list,
                         time: int, var_long: str,
                         cache=True) -> pd.DataFrame:
        """ Returns geomap of chosen variable (at single daytime) for several days

        :param files_list: list of files (.nc4) with datasets to process
        :param time: time of the day
        :param var_long: long name of the variable
        :param cache: flag to cache the dataset
        :return: dataframe with geomap of the chosen variable
        """

        res_df = pd.DataFrame()

        for current_file in files_list:

            # load dataset if not in cache
            if current_file not in self.ds_dict.keys():
                self._load_dataset(current_file)

            # get the geomap and add it to the result dataframe
            curr_df = self.ds_dict[current_file].time_slice(time, var_long)
            curr_df['FileName'] = current_file
            res_df = pd.concat([res_df, curr_df])

            # release memory occupied with dataset, if not cashed
            if not cache:
                del self.ds_dict[current_file]

        return res_df

