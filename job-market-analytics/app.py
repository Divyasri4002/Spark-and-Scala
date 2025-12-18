import sys, os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import streamlit as st
import plotly.express as px
from pyspark.sql.functions import col
from spark_utils import *

# ---------------- PAGE CONFIG ----------------
st.set_page_config(
    page_title="Job Market Analytics",
    page_icon="📊",
    layout="wide"
)

# ---------------- CUSTOM CSS ----------------
st.markdown("""
<style>
.metric {
    background: linear-gradient(135deg,#4e54c8,#8f94fb);
    padding: 18px;
    border-radius: 15px;
    color: white;
    text-align: center;
}
</style>
""", unsafe_allow_html=True)

# ---------------- SPARK INIT ----------------
spark = create_spark()
df = preprocess(load_data(spark))

# ---------------- SIDEBAR FILTERS ----------------
st.sidebar.header("🔍 Filters")

domains = [r.domain for r in df.select("domain").distinct().collect()]
cities = [r.city for r in df.select("city").distinct().collect()]

sel_domain = st.sidebar.multiselect("Domain", domains)
sel_city = st.sidebar.multiselect("City", cities)

filtered_df = df
if sel_domain:
    filtered_df = filtered_df.filter(col("domain").isin(sel_domain))
if sel_city:
    filtered_df = filtered_df.filter(col("city").isin(sel_city))

# ---------------- HEADER ----------------
st.title("📊 Job Market Data Analysis Dashboard")
st.markdown("### Career Planning & Market Demand Analytics using **PySpark + Streamlit**")

# ---------------- KPI METRICS ----------------
c1, c2, c3 = st.columns(3)

c1.markdown(f"<div class='metric'><h2>{filtered_df.count()}</h2>Total Jobs</div>",
            unsafe_allow_html=True)
c2.markdown(f"<div class='metric'><h2>{filtered_df.select('domain').distinct().count()}</h2>Domains</div>",
            unsafe_allow_html=True)
c3.markdown(f"<div class='metric'><h2>{filtered_df.select('skill').distinct().count()}</h2>Skills</div>",
            unsafe_allow_html=True)

st.divider()

# ---------------- INSIGHT BUTTONS ----------------
st.subheader("🎯 Explore Insights")

if "view" not in st.session_state:
    st.session_state.view = "domain"

b1, b2, b3, b4, b5, b6 = st.columns(6)

with b1:
    if st.button("📌 Domains"):
        st.session_state.view = "domain"
with b2:
    if st.button("🔥 Skills"):
        st.session_state.view = "skills"
with b3:
    if st.button("🎓 Experience"):
        st.session_state.view = "experience"
with b4:
    if st.button("💰 Salary"):
        st.session_state.view = "salary"
with b5:
    if st.button("📈 Trends"):
        st.session_state.view = "trend"
with b6:
    if st.button("📋 Jobs"):
        st.session_state.view = "table"

st.divider()

# ---------------- CONDITIONAL INSIGHTS ----------------
if st.session_state.view == "domain":
    st.subheader("📌 Jobs by Domain")
    st.plotly_chart(
         px.bar(domain_jobs(filtered_df).toPandas(),
             x="domain", y="jobs",
             color_discrete_sequence=["#FFDAB9"]),
        use_container_width=True
    )

elif st.session_state.view == "skills":
    st.subheader("🔥 Top Skills in Demand")
    skill_df = top_skills(filtered_df).toPandas()
    st.plotly_chart(
        px.bar(skill_df, x="skill", y="demand", color="demand"),
        use_container_width=True
    )

elif st.session_state.view == "experience":
    st.subheader("🎓 Experience Level Distribution")
    st.plotly_chart(
        px.pie(experience_jobs(filtered_df).toPandas(),
               names="experience_level", values="jobs"),
        use_container_width=True
    )

elif st.session_state.view == "salary":
    st.subheader("💰 Average Salary by Domain")
    st.plotly_chart(
        px.bar(salary_domain(filtered_df).toPandas(),
               x="domain", y="avg_salary"),
        use_container_width=True
    )

elif st.session_state.view == "trend":
    st.subheader("📈 Job Posting Trend Over Time")
    st.plotly_chart(
        px.line(job_trend(filtered_df).toPandas(),
                x="posteddate", y="jobs"),
        use_container_width=True
    )

elif st.session_state.view == "table":
    st.subheader("📋 Job Listings")
    st.dataframe(
        filtered_df.select(
            "jobtitle", "company", "domain", "city",
            "experience_level", "salary_min", "salary_max"
        ).toPandas(),
        use_container_width=True
    )

# ---------------- AUTO INSIGHTS ----------------
st.divider()
st.subheader("🧠 Key Insights")

top_domain = domain_jobs(filtered_df).first()["domain"]
top_skill = top_skills(filtered_df).first()["skill"]

st.success(f"📌 **{top_domain}** has the highest number of job openings.")
st.info(f"🔥 **{top_skill}** is the most demanded skill.")

# ---------------- SPARK UI ----------------
st.divider()
st.subheader("⚙️ Spark Execution Monitor")
st.markdown("🔗 **Spark Web UI:** http://localhost:4040")
st.caption("View Jobs, Stages, Executors, Storage & SQL execution")
