import pandas as pd

year = 2020

month = 7

#airport = "Kiruna"
airport = "Malmo"
#airport = "Ovik"
#airport = "Sundsvall"
#airport = "Umeo"


#metric1 = 'cbh'
#metric1_threshold = 61
#metric1_threshold = 500

#metric2 = 'lcc'
#metric2_threshold = 1
#metric2_threshold = 0.9


metric1 = 'cape'
#metric1_threshold = 150
metric1_threshold = 50

metric2 = 'cp'
#metric2_threshold = 0.3
metric2_threshold = 0.1
#threshold = 0.00025


input_csv_filename = 'data/' + airport + '/' + airport + '_' + metric1 + '_' + metric2 + '_' + str(year) + '_' + str(month) + '_ensemble.csv'
output_csv_filename = 'data/' + airport + '/probability_' + airport + '_' + str(year) + '_' + str(month) + '_' + metric1 + '_' + str(metric1_threshold) + '_'+ metric2 + '_' + str(metric2_threshold) +'.csv'



def calculateProbabilityConvectiveWeatherAboveThreshold(row):
    sum = 0
    for col in range(4,14):
        if metric1_threshold == 0:
            if row[col] > metric1_threshold:
                sum = sum + 1
        else:
            if row[col] >= metric1_threshold:
                sum = sum + 1
    for col in range(14,24):
        if row[col] >= metric2_threshold:
            sum = sum + 1
    return sum/20


def calculateProbabilityVisibilityBelowThreshold(row):
    sum = 0
    for col in range(4,14):
        if pd.isnull(row[col]):
            continue
        if row[col] <= metric1_threshold:
            sum = sum + 1
    for col in range(14,24):
        if row[col] >= metric2_threshold:
            sum = sum + 1
    return sum/20


df = pd.read_csv(input_csv_filename, sep=' ')

temp = df.isnull().sum()
print(temp)

if metric1 == "cbh":
    df['probability'] = df.apply(lambda row: calculateProbabilityVisibilityBelowThreshold(row), axis=1)
else:
    df['probability'] = df.apply(lambda row: calculateProbabilityConvectiveWeatherAboveThreshold(row), axis=1)

df = df[['date', 'hour', 'probability']]
df['date'] = df['date'].astype(int)


cols = ['date', 'hour', 'probability']
lst = []

for index, row in df.iterrows():

    previous_hour = row['hour']-1 if row['hour']>0 else 23
    date = row['date']  if row['hour']>0 else row['date']-1
    lst.append([date, previous_hour, row['probability']])
    
    lst.append([row['date'], row['hour'], row['probability']])
    
    next_hour = row['hour']+1
    lst.append([row['date'], next_hour, row['probability']])

new_df = pd.DataFrame(lst, columns=cols)

# remove the first row
new_df = new_df.drop(new_df.index[0])

# copy the last row
new_data = pd.DataFrame(new_df[-1:].values, columns=new_df.columns)
new_data['hour'] = 23
new_df = new_df.append(new_data)


#print(new_df.head())

new_df.to_csv(output_csv_filename, sep=' ', float_format='%.6f', encoding='utf-8', header=True, index=False)

