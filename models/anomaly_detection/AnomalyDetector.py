import pandas as pd
from adtk.detector import ThresholdAD, QuantileAD, InterQuartileRangeAD, GeneralizedESDTestAD, PersistAD, LevelShiftAD, VolatilityShiftAD, SeasonalAD, AutoregressionAD, MinClusterDetector, OutlierDetector, RegressionAD, PcaAD
from adtk.visualization import plot
from sklearn.cluster import KMeans
from sklearn.neighbors import LocalOutlierFactor
from sklearn.linear_model import LinearRegression

class AnomalyDetector():
    def __init__(self, data_scheme, size=10000):
        # initialize the data frame according to the scheme, which is a dictionary with column names and types
        self.data_scheme = data_scheme
        self.size = size
        
        # create an empty DataFrame with specified columns and data types, there must be a column named "timestamp" with datetime64 type
        if "timestamp" not in data_scheme:
            raise ValueError("The data scheme must contain a column named 'timestamp' with datetime64 type")
        
        self.historical_data = pd.DataFrame(columns=data_scheme.keys())
        for col, dtype in data_scheme.items():
            self.historical_data[col] = self.historical_data[col].astype(dtype)

        # use the timestamp as the index
        self.historical_data.set_index("timestamp", inplace=True)

    def detect_anomaly(self, new_data):
        # add new_data to the historical data
        self.add_history_data(new_data)

        # go through the data and detect anomalies
        anomaly_results = {}

        methods_with_params = [
            (ThresholdAD, {"high": 1, "low": 0}),
            (QuantileAD, {"high": 0.99, "low": 0.01}),
            (InterQuartileRangeAD, {"c": 1.5}),
            (GeneralizedESDTestAD, {"alpha": 0.3}),
            (LevelShiftAD, {"c": 6.0, "side": 'both', "window": 5}),
            (VolatilityShiftAD, {"c": 6.0, "side": 'positive', "window": 30}),
            (SeasonalAD, {"c": 3.0, "side": "both"}),
            (AutoregressionAD, {"n_steps": 7*2, "step_size": 24, "c": 3.0}),
            (MinClusterDetector, {"detector": KMeans(n_clusters=3)}),
            (OutlierDetector, {"detector": LocalOutlierFactor(contamination=0.05)}),
            (RegressionAD, {"regressor": LinearRegression(), "target": "Speed (kRPM)", "c": 3.0}),
            (PcaAD, {"k": 1}),
            ]
 
        for method, params in methods_with_params:
            detector = method(**params)
            if method == ThresholdAD:
                result = detector.detect(self.historical_data)
            else: 
                result = detector.fit_detect(self.historical_data)
                
            # trim the anomaly output to the size of the new_data
            result_trimmed = result.iloc[-len(new_data):] if len(result) > len(new_data) else result
            anomaly_results[method.__name__] = result_trimmed

        return anomaly_results

    def format_data(self, data):
        # format the data to be added to the historical data with correct data types
        formatted_data = pd.DataFrame(data, columns=self.data_scheme.keys())
        for col, dtype in self.data_scheme.items():
            formatted_data[col] = formatted_data[col].astype(dtype)
        formatted_data.set_index("timestamp", inplace=True)
        return formatted_data

    def add_history_data(self, data):
        # add the data to the historical data
        self.historical_data = pd.concat([self.historical_data, self.format_data(data)])
        
        # sort the data by the index
        self.historical_data = self.historical_data.sort_values(by="timestamp")
        
        # cut the data to the size
        if len(self.historical_data) > self.size:
            self.historical_data = self.historical_data.iloc[-self.size:]

      
