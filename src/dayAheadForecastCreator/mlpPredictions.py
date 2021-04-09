import pandas as pd 
import datetime as dt
import joblib
import numpy as np
import itertools
import tensorflow as tf


class MlpPredictions():
    """MLp prediction class
    """
    
    def __init__(self, modelPath: str) -> None:
        """load prediction model path
        Args:
            modelPath ([type]): path of model
        """
        self.modelPath = modelPath
        self.modelPathStr =""
        self.holidayListPathStr = modelPath +  '\\Holidays_List.xlsx'
        self.sortedTempStationList = ['station_43', 'station_11', 'station_10', 'station_15', 'station_12', 'station_14',
         'station_13', 'station_5', 'station_9', 'station_26', 'station_31', 'station_16', 'station_6', 'station_36', 
         'station_22', 'station_32', 'station_37', 'station_40', 'station_30', 'station_7', 'station_19', 'station_1', 
         'station_4', 'station_8', 'station_27', 'station_3', 'station_24', 'station_39', 'station_2', 'station_28', 
         'station_34', 'station_20', 'station_38', 'station_33', 'station_35', 'station_44', 'station_23', 'station_29',
         'station_25', 'station_21', 'station_41', 'station_17', 'station_42', 'station_18']
        self.holidayListDf = pd.read_excel(self.holidayListPathStr)
   

    def stationCombinationFinder(self, tempData, sortedStationList = None):
        
        #Reordering temperature dataframe
        reorderedTempData = tempData.reindex(columns = sortedStationList)
        #Creating combination for all the weather station temperature series (Simple Average)
        combinationDf = pd.DataFrame()
        for i in range(len(reorderedTempData.columns)):
            combinationDf["C" + str(i+1)] = reorderedTempData.iloc[:,0:i+1].mean(axis=1)
        return combinationDf
    
    
    def featureDfgenerator(self, demandData, combinationSet,
                       weatherCombinationCode= "C43"):
       
        rangeList = [np.arange(1,97) for i in range(int(len(demandData)/96))]
        timeblockList = itertools.chain.from_iterable(rangeList)
        CalenderDF = pd.DataFrame(index= demandData.index)
        CalenderDF['MONTH'] = demandData.index.month 
        CalenderDF['DAY'] = demandData.index.day 
        CalenderDF['HOUR'] = demandData.index.hour
        CalenderDF["TIMEBLOCK"] = list(timeblockList)
        CalenderDF['DAY-OF-WEEK'] = demandData.index.weekday
        CalenderDF["IS_WEEKEND"] = np.where((CalenderDF["DAY-OF-WEEK"] < 6), 0, 1)
        CalenderDF["HOLIDAY"] = np.isin(CalenderDF.index.date, self.holidayListDf.Date.dt.date).astype("int")
        CalenderDF["WEATHER"] = combinationSet["{}".format(weatherCombinationCode)]
        featureDF = pd.concat([CalenderDF, demandData], axis= 1)     
        return featureDF
          

    def predictDaMlp(self, lagDemandDf:pd.core.frame.DataFrame, weatherDf:pd.core.frame.DataFrame, entity:str)-> pd.core.frame.DataFrame:
        """predict DA forecast using model based on entity

        Args:
            lagDemandDf (pd.core.frame.DataFrame): dataframe containing blockwise D-2, D-7, D-14, D-21 demand with index timestamp of 'D'
            entity (str): entity tag like 'WRLDCMP.SCADA1.A0047000'

        Returns:
            pd.core.frame.DataFrame: DA demand forecast with column(timestamp, entityTag, demandValue)
        """    

        #setting model path string(class variable) based on entity tag(means deciding which model ti use)
        self.modelPathStr = self.modelPath + '\\' + str(entity) +'.h5'
        #Loading the model
        model_h5 = tf.keras.models.load_model(self.modelPathStr)
        
        
        # return daPredictionDf
        combinationDf =self.stationCombinationFinder(weatherDf, sortedStationList= self.sortedTempStationList)
        featureDf = self.featureDfgenerator(lagDemandDf, combinationDf)
        featureDfArr = featureDf.values.reshape(1,featureDf.shape[0], featureDf.shape[1])
        Yhat = model_h5.predict(featureDfArr, verbose= 0)
        daPredictionDf = pd.DataFrame(Yhat[0], index= featureDf.index)
        #renaming columns
        daPredictionDf.rename(columns={ 0: 'forecastedDemand'}, inplace=True)
        #adding entityTag column and resetting index
        daPredictionDf.insert(0, "entityTag", entity)  
        daPredictionDf.reset_index(inplace=True)
        return daPredictionDf
    
        

