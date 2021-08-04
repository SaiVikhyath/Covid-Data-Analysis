import math
import pandas as pd
import psycopg2
from matplotlib import pyplot
from configparser import ConfigParser
import sys

try:
        configFilePath = r'C:\Users\Mittu\Desktop\CovidDataAnalysis\Covid-Data-Analysis\Configurations\Configurations.ini'
        parser = ConfigParser()
        parser.read(configFilePath)
        database = parser.get('Database', 'database')
        user = parser.get('Database', 'user')
        password = parser.get('Database', 'password')
        host = parser.get('Database', 'host')
        port = parser.getint('Database', 'port')

except Exception as e:
        print("Cannot fetch configurations. Please check Configurations.ini")
        sys.exit()
        
try:
    conn = psycopg2.connect(database=database, user=user, password=password, host=host, port=port)
    cur = conn.cursor()
except Exception as e:
    print("Failed DB connectivity!",e)
    sys.exit()

VaccineData = pd.read_csv(r'C:\Users\Mittu\Desktop\CovidDataAnalysis\Covid-Data-Analysis\Datasets\covid_vaccine_statewise.csv')
#print(VaccineData.head())

IndiaVaccineData = VaccineData[VaccineData['State'] == 'Jammu and Kashmir']
#print(IndiaVaccineData.info())


print('\n\n*****************************************START**********************************************\n\n')
print('============================================================================================')
print('\n----------------------------Registration and Doses Statistics----------------------------\n')
print('============================================================================================')
registrations = int(IndiaVaccineData['Total Individuals Registered'].max())
firstDose = int(IndiaVaccineData['First Dose Administered'].max())
secondDose = int(IndiaVaccineData['Second Dose Administered'].max())
print('\nTotal registrations :', '{:,}'.format(registrations))
print('\nFirst dose administered :','{:,}'.format(firstDose))
print('\nSecond dose administered :','{:,}'.format(secondDose))
print('\nPercentage of people administered first dose of total registrations :', round((firstDose/registrations)*100,2))
print('\nPercentage of people vaccinated completely of total registrations :', round((secondDose/registrations)*100,2))
print('\nPeople who have to take the second dose :','{:,}'.format(firstDose-secondDose))
print('\nPercentage of people to take second dose of who have taken first dose :',round(((firstDose-secondDose)/firstDose)*100,2))
print('\nFirst dose administered per day(avg) :','{:,}'.format(math.ceil(firstDose/len(IndiaVaccineData))))
print('\nSecond dose administered per day(avg) :','{:,}'.format(math.ceil(secondDose/(len(IndiaVaccineData)-28))))

cur.execute('''insert into dose_distribution (state, first_dose, second_dose) 
values ((%s),(%s),(%s)) on conflict(state) do update set first_dose = (%s), second_dose = (%s)''',
('Jammu & Kashmir', firstDose, secondDose, firstDose, secondDose,))
conn.commit()

xPoints = ['First dose', 'Second dose']
yPoints = [firstDose, secondDose]
values = pd.DataFrame({'xPoints':xPoints, 'yPoints':yPoints})
pyplot.figure(figsize = (9,4))
pyplot.pie(yPoints, explode = (0,0.1), labels = xPoints, colors = ['yellow', 'orange'], autopct = '%1.1f%%', shadow = True)
pyplot.axis('equal')
pyplot.title('Doses administered')
pyplot.savefig(r'C:\Users\Mittu\Desktop\CovidDataAnalysis\Covid-Data-Analysis\Graphs\JammuAndKashmirDoses.png')
pyplot.ion()
pyplot.close()

xPoints = list(range(1,96))
yPoints = IndiaVaccineData['First Dose Administered'].sub(IndiaVaccineData['First Dose Administered'].shift())
yPoints = yPoints[1:]
pyplot.plot(xPoints, yPoints/1000)
pyplot.title('Daily administration of first dose')
pyplot.ylabel('First Dose Administered in thousands')
pyplot.xlabel('Days')
pyplot.savefig(r'C:\Users\Mittu\Desktop\CovidDataAnalysis\Covid-Data-Analysis\Graphs\JammuAndKashmirFirstDose.png')
pyplot.ion()
pyplot.close()

xPoints = list(range(1,96))
yPoints = IndiaVaccineData['Second Dose Administered'].sub(IndiaVaccineData['Second Dose Administered'].shift())
yPoints = yPoints[1:]
pyplot.plot(xPoints, yPoints/1000)
pyplot.title('Daily administration of second dose')
pyplot.ylabel('Second Dose Administered in thousands')
pyplot.xlabel('Days')
pyplot.savefig(r'C:\Users\Mittu\Desktop\CovidDataAnalysis\Covid-Data-Analysis\Graphs\JammuAndKashmirSecondDose.png')
pyplot.ion()
pyplot.close()


print('\n\n*******************************************END************************************************\n\n')
print('==============================================================================================')
print('\n-----------------------Vaccination statistics categorized by gender-----------------------\n')
print('==============================================================================================')
malesVaccinated = int(IndiaVaccineData['Male(Individuals Vaccinated)'].max())
femalesVaccinated = int(IndiaVaccineData['Female(Individuals Vaccinated)'].max())
transgendersVaccinated = int(IndiaVaccineData['Transgender(Individuals Vaccinated)'].max())
print('\nTotal males vaccinated :','{:,}'.format(malesVaccinated))
print('\nTotal females vaccinated :','{:,}'.format(femalesVaccinated))
print('\nTotal transgenders vaccinated :','{:,}'.format(transgendersVaccinated))

cur.execute('''insert into gender_distribution (state, males_vaccinated, females_vaccinated, transgenders_vaccinated) 
values ((%s),(%s),(%s),(%s)) on conflict(state) do update set  males_vaccinated = (%s), 
females_vaccinated = (%s), transgenders_vaccinated = (%s)''',
('Jammu & Kashmir', malesVaccinated, femalesVaccinated, transgendersVaccinated, malesVaccinated, femalesVaccinated, transgendersVaccinated,))
conn.commit()

xPoints = ['Males vaccinated', 'Females Vaccinated', 'Transgenders Vaccinated']
yPoints = [malesVaccinated/1000, femalesVaccinated/1000, transgendersVaccinated/1000]
pyplot.figure(figsize = (9,4))
pyplot.pie(yPoints, explode = (0,0.05,0.2), labels = xPoints, colors = ['blue', 'pink', 'orange'], autopct = '%1.1f%%', shadow = True)
pyplot.axis('equal')
pyplot.title('Vaccination categorization based on gender')
pyplot.savefig(r'C:\Users\Mittu\Desktop\CovidDataAnalysis\Covid-Data-Analysis\Graphs\JammuAndKashmirGender.png')
pyplot.ion()
pyplot.close()

print('\n\n*******************************************END************************************************\n\n')
print('==============================================================================================')
print('\n------------------------Vaccination statistics categorized by vaccine----------------------\n')
print('==============================================================================================')
covaxinAdministered = int(IndiaVaccineData['Total Covaxin Administered'].max())
covishieldAdministered = int(IndiaVaccineData['Total CoviShield Administered'].max())
print('\nTotal Covaxin doses administered :','{:,}'.format(covaxinAdministered))
print('\nTotal Covishield doses administered :','{:,}'.format(covishieldAdministered))

cur.execute('''insert into vaccine_distribution (state, covaxin_administered, covishield_administered)
 values ((%s),(%s),(%s)) on conflict(state) do update set covaxin_administered = (%s), covishield_administered = (%s)''',
 ('Jammu & Kashmir', covaxinAdministered, covishieldAdministered, covaxinAdministered, covishieldAdministered,))
conn.commit()

xPoints = ['Covaxin administered', 'Covishield Administered']
yPoints = [covaxinAdministered, covishieldAdministered]
pyplot.figure(figsize = (9,4))
pyplot.pie(yPoints, explode = (0,0.1), labels = xPoints, colors = ['blue', 'green'], autopct = '%1.1f%%', shadow = True)
pyplot.axis('equal')
pyplot.title('Categorization based on vaccine administered')
pyplot.savefig(r'C:\Users\Mittu\Desktop\CovidDataAnalysis\Covid-Data-Analysis\Graphs\JammuAndKashmirVaccine.png')
pyplot.ion()
pyplot.close()


print('\n\n*******************************************END************************************************\n\n')
print('==============================================================================================')
print('\n-------------Vaccination statistics of vaccination site and sessions conducted-------------\n')
print('==============================================================================================')
totalSessions = int(IndiaVaccineData['Total Sessions Conducted'].max())
totalSites = int(IndiaVaccineData['Total Sites '].max())
print('\nTotal Covid-19 vaccine sites :','{:,}'.format(totalSites))
print('\nTotal Covid-19 vaccine sessions :','{:,}'.format(totalSessions))
print('\nAverage number of sessions per site :',round(totalSessions/totalSites))
print('\nAverage number of first doses by a site :',round(firstDose/totalSites))
print('\nAverage number of second doses by a site :',round(secondDose/totalSites))
print('\nAverage number of first doses per session :',round(firstDose/totalSessions))
print('\nAverage number of second doses per session :',round(secondDose/totalSessions))
print('\nAverage number of doses by a site :',round((firstDose+secondDose)/totalSites))
print('\nAverage number of doses per session :',round((firstDose+secondDose)/totalSessions))