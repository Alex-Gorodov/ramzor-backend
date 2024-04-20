import numpy as np ,pandas as pd , os , random , pickle
from datetime import date
from datetime import datetime ,timedelta

class ShavzakBuilder:
    
    def __init__(self, df, mes, start_day=None, start_time="08:00", num_hours=48, add_officer=True):
        """
        Initialize a ShavzakBuilder object.

        Args:
            start_day (date, optional): The starting day for the schedule. Defaults to today.
            start_time (str, optional): The starting time in format "HH:MM". Defaults to "08:00".
            num_hours (int, optional): Total number of hours for the schedule. Defaults to 48.
            add_officer (bool, optional): Whether to add officers. Defaults to True.
        """
        if start_day is None:
            self.start_day = datetime.now()
        else:
            self.start_day = start_day
        self.df_all = df.copy()
        self.add_officer = add_officer
        if self.add_officer: 
            relavant_soldeirs = [0,1,2]
        else: 
            relavant_soldeirs = [0,1]
        
        # Old list with present.
        self.df = self.df_all.loc[ (self.df_all.present == 1) & (self.df_all.command.isin(relavant_soldeirs)) & (self.df_all.hamal ==0) & (self.df_all.maflag ==0)  ]
        print("Number of active people: %i" %(len(self.df)))
        # Take also people who are not present into ltm.
        self.df = self.df_all.loc[ (self.df_all.command.isin(relavant_soldeirs)) & (self.df_all.hamal ==0) & (self.df_all.maflag ==0)  ] 
        print("Number of all people: %i" %(len(self.df)))
        self.mes = mes.copy() 
        self.start_time = pd.to_datetime(start_time)
        self.num_hours = num_hours
        self.basic_time_step = 1
        self.end_date = pd.to_datetime(self.start_time) + pd.Timedelta(hours=(self.num_hours-1))
        timestamps = pd.date_range(start=self.start_time, end=self.end_date, freq='H')
        
        self.rest_indicator = 'rest'
        self.init_value = np.inf
        self.inactive_indicator = 'inactive'
        self.random_availability_init = True
        self.add_time_after_inactive = 0 # or np.inf. If inactive is like rest set to 0, if set mesima imediatly after inactive set to np.inf
        np.random.seed(1)
                
        # Create Ids folder and excel file for each id.
        self.ids_availability = {}
        date_list = []
        time_list = []
        for t in timestamps:
            print(t)
            if t._date_repr not in date_list:
                date_list.append(t._date_repr)
            if t._time_repr not in time_list:
                time_list.append(t._time_repr)
        
        for id in self.df['id']:
            print(id)
            if self.random_availability_init:
                data = (np.random.uniform(0, 1, [len(date_list), len(time_list)]) > 0.1 ).astype(int)
            else:               
                data = np.ones([len(date_list), len(time_list)])
                
            id_temp_DF = pd.DataFrame(data=data, columns = time_list)
            id_temp_DF = id_temp_DF.set_index(pd.Index(date_list))

            id_temp_DF.to_csv('ids/my_avail_'+str(id)+'.csv', index=True)
            self.ids_availability[id] = id_temp_DF
        
        del id, t
        
        self.ltm = pd.DataFrame()
        
        # Initialize ltm table.
        init_time = timestamps[0]._time_repr
        init_date = timestamps[0]._date_repr
        for id in list(self.df['id'].unique()):
            if (self.df[self.df['id']==id].present==1).values[0]:
                temp = pd.DataFrame({id: [[self.rest_indicator, self.init_value]]}).astype('object')
            elif self.df[self.df['id']==id].time_active.isnull().values[0]:
                temp = pd.DataFrame({id: [[self.inactive_indicator, -self.init_value]]}).astype('object')
            else:
                time_active = pd.to_datetime(((self.df[self.df['id']==id].time_active).values[0]).replace("\"",""))
                time_diff = (self.start_time - time_active)
                temp = pd.DataFrame({id: [[self.inactive_indicator, int(time_diff.total_seconds()/3600)]]}).astype('object')
            
            if self.ids_availability[id].loc[init_date][init_time] == 1:
                temp = pd.DataFrame({id: [[self.rest_indicator, self.init_value]]}).astype('object')
            else:
                temp = pd.DataFrame({id: [[self.inactive_indicator, self.init_value]]}).astype('object')
            
            self.ltm = pd.concat([self.ltm, temp], axis=1)
            
        #self.ltm = pd.DataFrame({i: [[self.rest_indicator, self.init_value]] for i in list(self.df['id'].unique()) }).astype('object')
        self.ltm = pd.concat([self.ltm] * self.num_hours, ignore_index=True)
        self.ltm = self.ltm.set_index(pd.Index(timestamps)) 
        
        self.shavzak  = pd.DataFrame(columns = ['Date' , 'hour' , 'mesima' , 'type' , 'names'])
        self.unique_mesima = mes['name'].unique()
        self.last_set  = {key: {} for  key in self.unique_mesima}
        self.add_random = 0 # 2 
        self.seed = 1920

    def build_ltm(self):
        
        """
        Build a shavzak schedule based on provided parameters.

        Returns:
            pd.DataFrame: A DataFrame representing the schedule.
        """
        
        total_time = 0
        time_indicis_list = list(self.ltm.index)
        
        for i in np.arange(0, len(time_indicis_list)-1):
            target_index = time_indicis_list[i]
            next_index = time_indicis_list[i+1]
            
            self.ids_updated = []
    
            for mesima in self.unique_mesima: 
            
                info_mes = self.mes.loc[self.mes.name== mesima]
                duration = int(info_mes.duration.values[0])
                #info_columns = list(info_mes.columns)
                #info_columns = [x for x in info_columns if x not in ['duration' , 'name' , 'one_time']]
                
                # Perpetum and cyclical mesima from start to end without specifying anything:
                if self.mes.loc[self.mes.name== mesima].start_time.isnull().values[0]:
                    if not (total_time%duration == 0):
                        continue
                # One time mesima or daily masima at exact time or cyclical daily masima at exact time and bounded by time:
                else:
                    time_mesima = self.mes.loc[self.mes.name== mesima].start_time.values[0].replace("\"","").replace(" ","")
                    date_ = target_index.date()
                    # Cyclical daily masima at exact time (unbounded). Add the current date to the time of mesima and compare to target_index.
                    if self.mes.loc[self.mes.name== mesima].start_date.isnull().values[0]:
                        date_start_mesima = date_
                        full_time_mesima = pd.to_datetime(str(date_) + ' ' + str(time_mesima))
                    # Bounded cyclical mesima.
                    else:
                        date_start_mesima = self.mes.loc[self.mes.name== mesima].start_date.values[0].replace("\"","").replace(" ","")
                        # No end_date cyclical or one time
                        if self.mes.loc[self.mes.name== mesima].end_date.isnull().values[0]:
                            # Cyclical
                            if self.mes.loc[self.mes.name== mesima].one_time_activity.isnull().values[0] or self.mes.loc[self.mes.name== mesima].one_time_activity.values[0]==0:
                                date_end_mesima = str(date_)
                            # One time
                            else:
                                date_end_mesima = str(date_start_mesima)
                        else:
                            date_end_mesima = self.mes.loc[self.mes.name== mesima].end_date.values[0].replace("\"","").replace(" ","")
                        
                        if  date_ >= datetime.strptime(date_start_mesima,'%Y-%m-%d').date()  and date_ <= datetime.strptime(date_end_mesima,'%Y-%m-%d').date():
                            full_time_mesima = pd.to_datetime(str(date_) + ' ' + str(time_mesima))
                        else:
                            continue
                            
                    if full_time_mesima != target_index:
                        continue
                    
                print('add mesima: %s at %s - duration %f' %(mesima, str(target_index), duration))
                
                for ns in ['soldier','command']:
                        
                    n = info_mes[ns].values[0]
                        
                    # relevant ids
                    self.get_relevant_ids(target_index, n , ns, duration, i, time_indicis_list)
                    
                    self.ids_updated += list(self.df_ids_random.sorted_ids.values)

                    self.update_mesima(target_index, self.df_ids_random.sorted_ids.values, mesima, duration)
                        
            # update duration for the next time stemp.
            self.update_duration(next_index, target_index) # , self.ids_updated) #mesima, duration)         

            total_time += self.basic_time_step 
        
        del ns, mesima  
        self.mes.to_csv(os.getcwd()+ '/temp/mesimot.csv')
        self.ltm.to_csv(os.getcwd()+ '/temp/ltm.csv')
        print("*************** end building ltm **************")
        '''
        # Debug
        for id in self.ltm.loc[self.ltm.index[5]].index:
            print("%i %s" %(id, str(self.ltm.loc[self.ltm.index[5]][id])))
        '''

    def get_relevant_ids(self , target_index, n , ns, duration, time_index_start, time_indicis_list):
        command_to_number = 0 if ns == 'soldier' else 1 if ns == 'command' else 'error'
        assert command_to_number !='error'
        #relevant_ids = list(self.df.loc[(self.df.command == command_to_number) &  (self.df.active == 0)].id.copy())
        relevant_ids_temp = list(self.df.loc[(self.df.command == command_to_number)].id.copy())
        # check if id is present and at rest?
        time_index_end = int(time_index_start + duration/self.basic_time_step - 1)
        times_mesima = time_indicis_list[time_index_start:time_index_end]
        relevant_ids = []
        for id in relevant_ids_temp:
            avail = 1
            # Id is available at the start of the mesima.
            if self.ltm.loc[target_index , id][0] != self.rest_indicator:
                avail = 0
            # Check if 
            else:
                for t in times_mesima:
                    avail *= self.ids_availability[id].loc[t._date_repr, t._time_repr]
            if avail == 1:
            #if self.ltm.loc[target_index , id][0] != self.rest_indicator:
                relevant_ids.append(id)
        
        assert len(relevant_ids_temp) >= len(relevant_ids)
        assert len(relevant_ids) >= n
        
        list_of_duration = []
        for id in relevant_ids:
            list_of_duration.append(-self.ltm.loc[target_index , id][1])
                
        # Sort ids based on the duration
        sorted_duration, sorted_ids = zip( *sorted(zip(list_of_duration, relevant_ids)))
        df_sorted = pd.DataFrame({'sorted_duration': list(sorted_duration), 'sorted_ids':list(sorted_ids) }).reset_index()
        
        sorted_duration_extent =  df_sorted.head(n + self.add_random)
        self.df_ids_random = sorted_duration_extent.sample(n , random_state = self.seed)
        
    def update_mesima(self, target_index, ids, mesima, duration):
        for col in ids:
            self.ltm.at[target_index, col] = update_cells(self.ltm.at[target_index, col], mesima, add_duration = -duration)
    
    def update_duration(self, next_index, target_index):
        for id in self.ltm.columns:            
            # Copy all status to next time step and update the times by one time step.
            self.ltm.at[next_index, id] = update_cells(self.ltm.at[target_index, id], self.ltm.at[target_index, id][0], add_duration = self.basic_time_step)
            # If id is not available next time set it to inactive and time inf:
            if self.ids_availability[id].loc[next_index._date_repr, next_index._time_repr] == 0:
                self.ltm.at[next_index, id] = update_cells(self.ltm.at[next_index, id], self.inactive_indicator, add_duration = self.add_time_after_inactive)
            else:
                # If id was inactive and now turns into active set it to rest (now without changing its time so it is like rest or with inf time):
                if self.ltm.at[next_index, id][0] == self.inactive_indicator:
                    self.ltm.at[next_index, id] = update_cells(self.ltm.at[next_index, id], self.rest_indicator, add_duration = self.add_time_after_inactive)  
                else:
                    # If timestemp equals zero, mesima is over so set id's mesima to rest.
                    if self.ltm.at[next_index, id][1] == 0:
                    #if self.ltm.at[next_index, id][0] != self.inactive_indicator:
                        self.ltm.at[next_index, id] = update_cells(self.ltm.at[next_index, id], self.rest_indicator, add_duration = 0)

    def add_to_shavzak(self, mesima , ns ,time_ ): 
        list_as_string = ', '.join(map(str, self.relevant_names_fix_number))
        final_info = [str(self.start_day), str(time_.strftime("%H:%M")) , mesima , ns , list_as_string ]
        self.shavzak.loc[len(self.shavzak)] = final_info 

    def print_status_report (self):
        df_report = self.df_all
        n_present = df_report.loc[(df_report['present'] == 1) & (self.df_all.command.isin([0,1,2])) ]['id'].nunique()
        n_total = df_report['id'].nunique()
        n_hamal = df_report.loc[df_report.hamal == 1]['id'].nunique()
        n_maflag = df_report.loc[df_report.maflag == 1]['id'].nunique()
        
        print ( f' total soldiers for mesimot = {n_total} \n total present for mesimot = {n_present} \n percent present for mesimot {np.round (n_present / n_total , 2) *100} \n hamal = {n_hamal} \n maflag = {n_maflag}'    )
   
def update_cells(lst, mesima , add_duration = None):
    if add_duration < 0:
        return [mesima, add_duration]        
    else: 
        return [mesima, lst[1] + add_duration]
    
print( f'path: {os.getcwd()}') 
print("Today's date:", date.today())

haim=0
if haim==1:
    path_ids = r'C:\Users\natano\Documents\ids.csv'
    path_mesimot = r'C:\Users\natano\Documents\mesimot1.csv'
else:
    path_ids = os.getcwd() + "/" + "ids.csv"
    path_mesimot = os.getcwd() + "/" + "mesimot1.csv"

print( f'path_ids: {path_ids}')
print( f'path_mesimot: {path_mesimot}')


df = pd.read_csv(path_ids)
mes = pd.read_csv(path_mesimot) 

df['Name'] = df['name']+ '_' + df['famely'] 
mes


start_day = date.today()
start_time = "2023-10-20 12:00:00"
num_hours = 120
add_officer = True

s = ShavzakBuilder(df, mes,start_day, start_time , num_hours , add_officer )
#s.save_and_load_ltm()
s.build_ltm()




print("sof tov hakol tov")

