from PIL import Image
from io import BytesIO
import requests
import streamlit as st
import psycopg2
from kafka import KafkaConsumer
import simplejson as json
from datetime import datetime
import pandas as pd
import numpy


@st.cache_data
def fetch_voting_stats():
    conn = psycopg2.connect(
        "host=localhost dbname=voting user=postgres password=postgres")
    cur = conn.cursor()

    # Fetch total nubmer of voters
    cur.execute("""
                SELECT count(*) voters_count FROM voters
                """)
    voters_count = cur.fetchone()[0]

    # Fetch totall nubmer of candidates
    cur.execute("""
                SELECT count(*) candidates_count FROM candidates
                """)

    candidates_count = cur.fetchone()[0]
    return voters_count, candidates_count


def create_kafka_consumer(topic_name):
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=["localhost:9092"],
        auto_offset_reset="earliest",
        value_deserializer=lambda x: json.loads(x.decode("utf-8"))
    )
    return consumer


def fetch_kafka_consumer(consumer):
    messages = consumer.poll(timeout_ms=1000)
    data = []

    for msg in messages.values():
        for sub_msg in msg:
            data.append(sub_msg.value)

    return data


def update_data():
    now = datetime.now()
    last_refresh = st.empty()
    last_refresh.text(f"Last refresh at: {now.now()}")

    # fetch voting statistics from postgres
    voters_count, candidates_count = fetch_voting_stats()

    # Display the statistics
    st.markdown("""---""")
    col1, col2 = st.columns(2)
    col1.metric("Total Voters", voters_count)
    col2.metric("Total Candidates", candidates_count)

    consumer = create_kafka_consumer(topic_name)
    data = fetch_kafka_consumer(consumer)

    results = pd.DataFrame(data)

    HOWTOP = 5
    # Identify the leading cnadidate
    results = results.loc[results.groupby(
        "candidate_id")["total_votes"].idxmax()]
    results = results.sort_values("total_votes", ascending=False).head(HOWTOP)

    st.header("leading Cnadidate : TOP 5 ")
    st.markdown("""---""")

    for i in range(HOWTOP):
        col1, col2 = st.columns(2)
        with col1:
            # show images of candidates
            url = list(results["photo_url"])[i]
            response = requests.get(url)
            img = Image.open(BytesIO(response.content))
            st.image(img, width=200)

        with col2:
            st.subheader(list(results["candidate_name"])[i])
            st.header(list(results["party_affiliation"])[i])
            st.subheader("Total Vote : {}".format(
                list(results["total_votes"])[2]))
        st.markdown("""---""")


# MIAN
st.title("RealTime Voting Dashboard")
topic_name = "aggregated_votes_per_candidate"
update_data()
