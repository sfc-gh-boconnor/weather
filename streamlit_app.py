# Import python packages
import streamlit as st
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col,avg,sum,max,div0, current_date, object_construct, array_agg, concat,lit, call_function
from snowflake.snowpark.types import StringType
import pandas as pd

st.set_page_config(layout="wide")



### header info
col1,col2 = st.columns([0.1,0.9])


with col1:
    st.image('https://upload.wikimedia.org/wikipedia/en/thumb/f/f4/Met_Office.svg/1200px-Met_Office.svg.png')

with col2:
    st.markdown(
    """
     🎉 **Congratulations Met Office** 🎉 for publishing your first dataset to the Snowflake Cloud Market place.
    """
    )
    st.subheader('UK Weather Forecast for a chosen Postcode Sector')


# Get the current credentials
session = get_active_session()


### the form to put in postcode sector and the model.  Hint..... Mixtral is the best one that does the job
with st.form('weather'):
    col1,col2 = st.columns(2)
    with col1:
        sector = st.text_input('Postcode Sector: ','DY13_9')
    with col2:
        model = 'mixtral-8x7b','reka-flash', 'gemma-7b','llama2-70b-chat'
        select_model = st.selectbox('Choose Model',model)
    submit_forecast = st.form_submit_button('View the Forecast')



if submit_forecast:

    ##### grab the met office table - you need to get it from the market place first.

    
    weather = session.table('POSTCODE_SECTOR_WEATHER_FORECASTS.BD1_BETA."advanced_with_solar_daily_view"')

    #### find the latest time that the forecast was issued at
    weather_max = weather.agg(max('"Issued_at"')).collect()[0][0]



    st.write(f'''#### Latest weather forecast as of **{weather_max}**''')


    #### filter the data to the chosen postcode sector - the format is like this - SN25_2


    weather_filter = weather.filter((col('PC_SECT')==sector) & (col('"Issued_at"')==weather_max))
    weather_filterpd = weather_filter.to_pandas()


    ##### filter to view todays weather forecast
    today = weather_filter.filter(col('"Validity_date"')==current_date())

    st.caption('This is the live uptodate weather forecast for today')

    st.dataframe(today)
    todaypd = today.to_pandas()

    st.divider()

    st.caption(f'And **this** is how {select_model} explains what it means')

    ##### melt all the columns into rows - i quickly did this with pandas melt
    melt = pd.melt(todaypd)
    melt['variable'] = melt['variable'].astype("string")
    melt['value'] = melt['value'].astype("string")

    ##### create an object to feed into the LLM
    object = session.create_dataframe(melt)
    object = object.with_column('object',object_construct(col('"variable"'),col('"value"')))

    object = object.select(array_agg('OBJECT').alias('OBJECT'))

    ##### create an LLM prompt which includes the data object
    prompt = concat(lit('Can you explain to me what the weather is like based on this data'),
                    col('object').astype(StringType()),
                   lit('as you explain, incorporate weather related emojis to the generated text to help illustrate the text further'))
    
    
    # construct the cortex.complete function - this will run based on the chosen model
    complete = call_function('snowflake.cortex.complete',lit(select_model),lit(prompt))

    object = object.select(complete)

    #### print the results
    st.write(object.collect()[0][0])

    st.divider()

    #### below are a series of standard streamlit line charts showing the weather stats for the next 14 days
    st.markdown('##### FORECAST STATS FOR THE NEXT 14 DAYS')

    col1,col2,col3 = st.columns(3)

    with col1:

        weather_filterpd['Max Temp Day']=weather_filterpd['Max_temperature_day']
        weather_filterpd['Min Temp Night']=weather_filterpd['Min_temperature_night']

        st.markdown('###### TEMPERATURE')
        chart1 = st.line_chart(weather_filterpd, 
             x= "Validity_date", 
             y=["Max Temp Day" ,
                "Min Temp Night"
                ])

    with col2:

        weather_filterpd['day']=weather_filterpd['Probability_of_Sunshine_day']
        weather_filterpd['night']=weather_filterpd['Probability_of_Clear_Skies_night']

   

        st.markdown('###### PROBABILITY OF SUNSHINE / CLEAR SKIES')
        chart1 = st.line_chart(weather_filterpd, 
             x= "Validity_date", 
             y=["day",
                "night"
               ])

    with col3:

        weather_filterpd['day']=weather_filterpd['Wind_gust_Approx_Local_Midday']
        weather_filterpd['night']=weather_filterpd['Wind_gust_Approx_Local_Midnight']

   

        st.markdown('###### WIND GUST')
        chart1 = st.line_chart(weather_filterpd, 
             x= "Validity_date", 
             y=["day",
                "night"
               ])


    with col1:

        weather_filterpd['day']=weather_filterpd['Probability_of_Rain_day']
        weather_filterpd['night']=weather_filterpd['Probability_of_Rain_night']

        st.markdown('###### PROBABILITY OF RAIN')
        chart1 = st.line_chart(weather_filterpd, 
             x= "Validity_date", 
             y=["day" ,
                "night"
                ])


    with col2:

        weather_filterpd['day']=weather_filterpd['Probability_of_Mist_day']
        weather_filterpd['night']=weather_filterpd['Probability_of_Mist_night']

   

        st.markdown('###### PROBABILITY OF MIST')
        chart1 = st.line_chart(weather_filterpd, 
             x= "Validity_date", 
             y=["day",
                "night"
               ])

    with col3:

        weather_filterpd['day']=weather_filterpd['Probability_of_Precipitation_day']
        weather_filterpd['night']=weather_filterpd['Probability_of_Precipitation_night']

        st.markdown('###### PROBABILITY OF PRECIPITATION')
        chart1 = st.line_chart(weather_filterpd, 
             x= "Validity_date", 
             y=["day",
                "night"
               ])



