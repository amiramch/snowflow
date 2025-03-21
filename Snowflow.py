import streamlit as st
from snowflake.snowpark.context import get_active_session
import pandas as pd
import numpy as np
import plotly.express as px

session = get_active_session()

st.set_page_config(
    page_title="Consumption",
    page_icon="❄️",
    layout = 'wide'
)

# Load datasets
@st.cache_data
def load_data_events():
    return session.table('events').to_pandas()

@st.cache_data
def load_data_users():
    return session.table('users').to_pandas()


@st.cache_data
def campaigns():
    return session.table('campaigns').to_pandas()

events_df = load_data_events()
users_df = load_data_users()
campaigns_df = campaigns()

# Sidebar - Campaign Definition
st.sidebar.header("Load Campaign")
campaign_name_load = st.sidebar.selectbox("Campaign Name",campaigns_df['NAME'].unique())

# Sidebar - Campaign Definition
st.sidebar.header("Create Campaign")
campaign_name = st.sidebar.text_input("Campaign Name", "")

# Select number of filter sets
num_sets = st.sidebar.number_input("Number of Filter Sets", min_value=1, max_value=10, value=1, step=1)

# Set combination logic (AND / OR)
set_combination = st.sidebar.radio("Combine Sets Using", ["AND", "OR"])

# Store user-defined sets
filter_sets = []

# Loop through the number of sets dynamically
for i in range(num_sets):
    with st.sidebar.expander(f"Set {i+1} - Audience Rules", expanded=(num_sets == 1)):
        event_type_mode = st.radio(f"Event Type Mode (Set {i+1})", ["Include", "Exclude"], key=f"mode_{i}")
        selected_event_types = st.multiselect(f"Select Event Type (Set {i+1})", events_df["EVENT_TYPE"].unique(), key=f"event_{i}")
        selected_device = st.multiselect(f"Device (Set {i+1})", events_df["DEVICE"].unique(), key=f"device_{i}")
        selected_plan = st.multiselect(f"Plan (Set {i+1})", users_df["PLAN"].unique(), key=f"plan_{i}")

        start_date, end_date = st.date_input(f"Event Date Range (Set {i+1})",
                                             [events_df["TIMESTAMP"].min(), events_df["TIMESTAMP"].max()],
                                             key=f"date_{i}")
        
        min_event_count = st.slider(f"Min Event Occurrences (Set {i+1})", 1, 50, 5, key=f"count_{i}")
        active_in_last_days = st.slider(f"Active Users in Last N Days (Set {i+1})", 7, 90, 30, key=f"days_{i}")

        filter_sets.append({
            "mode": event_type_mode,
            "event_types": selected_event_types,
            "device": selected_device,
            "plan": selected_plan,
            "start_date": start_date,
            "end_date": end_date,
            "min_event_count": min_event_count,
            "active_in_last_days": active_in_last_days
        })

# Function to apply filters for a single set
def filter_users(events_df, users_df, event_type_mode, selected_event_types, selected_device, selected_plan, start_date, end_date, min_event_count, active_in_last_days):
    filtered_events = events_df.copy()
    
    if selected_event_types:
        if event_type_mode == "Include":
            filtered_events = filtered_events[filtered_events["EVENT_TYPE"].isin(selected_event_types)]
        else:
            filtered_events = filtered_events[~filtered_events["EVENT_TYPE"].isin(selected_event_types)]

    if selected_device:
        filtered_events = filtered_events[filtered_events["DEVICE"].isin(selected_device)]
    if selected_plan:
        filtered_events = filtered_events[filtered_events["PLAN_AT_EVENT"].isin(selected_plan)]

    filtered_events = filtered_events[
        (filtered_events["TIMESTAMP"] >= pd.Timestamp(start_date)) &
        (filtered_events["TIMESTAMP"] <= pd.Timestamp(end_date))
    ]

    # Aggregate User Activity
    user_event_counts = filtered_events.groupby("USER_ID")["EVENT_ID"].count().reset_index()
    user_event_counts.columns = ["USER_ID", "event_count"]

    users_active_recently = users_df[
        pd.to_datetime(users_df["LAST_LOGIN"]) >= pd.Timestamp.today() - pd.Timedelta(days=active_in_last_days)
    ]

    # Merge Data
    users_df["USER_ID"] = users_df["USER_ID"].astype(str)
    user_event_counts["USER_ID"] = user_event_counts["USER_ID"].astype(str)

    filtered_users = users_df.merge(user_event_counts, on="USER_ID", how="left").fillna({"event_count": 0})
    filtered_users = filtered_users[filtered_users["event_count"] >= min_event_count]

    filtered_users["USER_ID"] = filtered_users["USER_ID"].astype(str)
    users_active_recently["USER_ID"] = users_active_recently["USER_ID"].astype(str)
    return filtered_users[filtered_users["USER_ID"].isin(users_active_recently["USER_ID"])]

# Apply filters dynamically for all sets
filtered_sets = []
for filter_set in filter_sets:
    filtered_sets.append(filter_users(
        events_df, users_df, filter_set["mode"], filter_set["event_types"], 
        filter_set["device"], filter_set["plan"], filter_set["start_date"], 
        filter_set["end_date"], filter_set["min_event_count"], filter_set["active_in_last_days"]
    ))

# Combine all sets based on the user’s choice
if set_combination == "AND":
    final_filtered_users = filtered_sets[0]
    for i in range(1, len(filtered_sets)):
        final_filtered_users = final_filtered_users.merge(filtered_sets[i], on="USER_ID", how="inner")
else:
    final_filtered_users = pd.concat(filtered_sets).drop_duplicates()


final_filtered_users["Country"] = final_filtered_users.get("COUNTRY", final_filtered_users.get("COUNTRY_x", ""))
final_filtered_users["Device"] = final_filtered_users.get("DEVICE", final_filtered_users.get("DEVICE_x", ""))
final_filtered_users["Plan"] = final_filtered_users.get("PLAN", final_filtered_users.get("PLAN_x", ""))
final_filtered_users["Num_Tasks"] = final_filtered_users.get("NUM_TASKS", final_filtered_users.get("NUM_TASKS_x", ""))
final_filtered_users["Email"] = final_filtered_users.get("EMAIL", final_filtered_users.get("EMAIL_x", ""))


# Display Results
st.markdown(f"""<span style="color:#405963;font-size:30px">**Filtered Audience for: {campaign_name}**</span>""", unsafe_allow_html=True) 
st.metric("Matching Users", f"{len(final_filtered_users):.0f}")

####################### UI INSIGHTS ######################

dis1,dis0,dis2 = st.columns([1.0,0.1,1.0])

with dis1:
    fil1,fil2,fil3 = st.columns(3)
    dimension = fil1.selectbox("Group by:", ["Country", "Device", "Plan"])
    top_n = fil2.number_input(f"Top {dimension}:", min_value=1, max_value=20, value=10, step=1)
    bar_color = fil3.color_picker("Pick a bar color", "#1f77b4")
    
    user_counts = final_filtered_users[dimension].value_counts().reset_index()
    user_counts.columns = [dimension, "User Count"]
    user_counts = user_counts.nlargest(top_n, "User Count").sort_values(by="User Count", ascending=True)  # Show top N values

    
    fig_bar = px.bar(user_counts, y=dimension, x="User Count", title=f"Top {top_n} User Count by {dimension}",
                     color_discrete_sequence=[bar_color], text="User Count", orientation="h")  # Horizontal bar chart
    
    fig_bar.update_traces(textposition="outside")  # Show value labels
    
    fig_bar.update_layout(
        xaxis_title='', 
        yaxis_title='') 

    
    st.plotly_chart(fig_bar)

with dis2:
    
    random_y_values = np.random.uniform(low=0, high=1, size=len(final_filtered_users))  # Random values between 0 and 1
    
    avg_tasks = final_filtered_users["Num_Tasks"].mean()


    fig_scatter = px.scatter(final_filtered_users, x="Num_Tasks", y=random_y_values,
                             color=final_filtered_users[dimension], title="Task Distribution per Customer",
                             opacity=0.7,
                             color_discrete_map={val: px.colors.qualitative.Set1[i % len(px.colors.qualitative.Set1)]
                                                 for i, val in enumerate(final_filtered_users[dimension].unique())})
    
    fig_scatter.update_traces(marker=dict(size=10))  # Set all circles to the same size

    

    
    fig_scatter.update_traces(marker=dict(symbol="circle"))
    fig_scatter.update_layout(yaxis=dict(showticklabels=False), height=500)  # Adjustable height

    
    fig_scatter.update_layout(
        xaxis_title='Number of Tasks', 
        yaxis_title='') 
    
    st.plotly_chart(fig_scatter)


###########################################################

st.markdown(f"""<span style="color:#405963;font-size:22px">**Users List**</span>""", unsafe_allow_html=True) 
st.dataframe(final_filtered_users)

line_color = "#326377"  # Change the color of the lines
line_width = "2px"  # Change the width of the lines


# Add the top line
st.write('')
st.markdown(f"""<span style="color:#405963;font-size:22px">**Actions**</span>""", unsafe_allow_html=True) 
st.markdown(f"<hr style='border: {line_width} solid {line_color};'>", unsafe_allow_html=True)


b1,b2,b3,b0=st.columns([1.0,1.0,1.0,3.0])
b1.download_button("Download Audience List :arrow_down:", final_filtered_users.to_csv(index=False), "filtered_audience.csv", "text/csv")
sf = b2.button("Sync with Salesforce :snowflake:")
pm = b3.button("Personalized Message :envelope_with_arrow:")


# if sf:
#     table_name = 'Campaign'
#     snowpark_df = session.create_dataframe(final_filtered_users)
#     snowpark_df.write.mode("overwrite").save_as_table(table_name)
#     sync = session.sql(f"CALL google_sheets_api_write('{table_name}')").collect()
    

if sf:
    # Show "in progress" message
    with st.spinner('Syncing campaign details with Salesforce...'):
        # Perform sync operations
        table_name = 'Campaign'
        snowpark_df = session.create_dataframe(final_filtered_users)
        snowpark_df.write.mode("overwrite").save_as_table(table_name)
        sync = session.sql(f"CALL google_sheets_api_write('{table_name}')").collect()
    
    # Show success message after completion
    st.success('✅ Campaign details successfully synced with Salesforce!')


st.markdown(f"<hr style='border: {line_width} solid {line_color};'>", unsafe_allow_html=True)

save = st.sidebar.button("Save Campaign", type = 'primary')

if save:
    commit = session.sql(f"insert into snowflow.raw.campaigns (name) values ('{campaign_name}')").collect()
    st.sidebar.success("Campaign Setting Saved")
    st.cache_data.clear()  # Clear cache to force a re-fetch


# Debugging: Show selected filters
with st.sidebar.expander(":blue[Selected Rules Summary]"):
    st.write(f"**Campaign Name:** {campaign_name}")
    st.write(f"**Set Combination Mode:** {set_combination}")
    for idx, filter_set in enumerate(filter_sets):
        st.write(f"### Set {idx+1} Rules")
        st.write(f"**Event Type Mode:** {filter_set['mode']}")
        st.write(f"**Event Types:** {filter_set['event_types'] or 'All'}")
        st.write(f"**Device:** {filter_set['device'] or 'All'}")
        st.write(f"**Plan:** {filter_set['plan'] or 'All'}")
        st.write(f"**Event Date Range:** {filter_set['start_date']} to {filter_set['end_date']}")
        st.write(f"**Min Event Count:** {filter_set['min_event_count']}")
        st.write(f"**Active in Last Days:** {filter_set['active_in_last_days']}")
