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

    # Identify the leading cnadidate
    results = results.loc[results.groupby(
        "candidate_id")["total_votes"].idxmax()]
    leading_cnadidate = results.loc[results["total_votes"]].idxmax()

    st.markdown("""---""")
    st.header("leading Cnadidate")
    col1, col2 = st.columns(2)

    with col1:
        # st.image(leading_cnadidate["photo_url"], width=200)
        st.subheader(leading_cnadidate["candidate_name"])
    with col2:
        st.header(leading_cnadidate["candidate_name"])
        st.subheader(leading_cnadidate["party_affiliation"])
        # st.subheader("Total Vote : {}".format(
        #     leading_cnadidate["total_votes"]))


# MIAN
st.title("RealTime Voting Dashboard")
topic_name = "aggregated_votes_per_candidate"
update_data()
