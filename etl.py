import pandas as pd
from datetime import date
from db import get_connection

def extract():

    conn = get_connection()
    query = """
     
          SELECT j.id, j.salary_min, j.salary_max, j.location, s.name as skill
        FROM jobs j
        LEFT JOIN job_skills js ON j.id = js.job_id
        LEFT JOIN skills s ON js.skill_id = s.id


      """

    df = pd.read_sql(query, conn)
    conn.close()
    return df


def transform(df):

    today = date.today()

    # Normalise salaries — divide by 12 if likely annual
    df['salary_min'] = df.apply(lambda r: r['salary_min'] / 12 if r['salary_max'] and r['salary_max'] > 100000 else r['salary_min'], axis=1)
    df['salary_max'] = df.apply(lambda r: r['salary_max'] / 12 if r['salary_max'] and r['salary_max'] > 100000 else r['salary_max'], axis=1)

   # Skill demand — count jobs per skill
    skill_demand = df.groupby('skill')['id'].nunique().reset_index()
    skill_demand.columns = ['skill', 'job_count']
    skill_demand['snapshot_date'] = today


    # Salary per skill
    salary = df[df['salary_min'].notna()].groupby('skill').agg(
        avg_min=('salary_min', 'mean'),
        avg_max=('salary_max', 'mean')
    ).reset_index()
    salary['avg_min'] = salary['avg_min'].round().astype(int)
    salary['avg_max'] = salary['avg_max'].round().astype(int)
    salary['snapshot_date'] = today


      # Location — map city to province
    province_map = {
        'johannesburg': 'Gauteng', 'sandton': 'Gauteng', 'pretoria': 'Gauteng',
        'midrand': 'Gauteng', 'centurion': 'Gauteng', 'randburg': 'Gauteng',
        'cape town': 'Western Cape', 'stellenbosch': 'Western Cape',
        'durban': 'KwaZulu-Natal', 'umhlanga': 'KwaZulu-Natal',
        'port elizabeth': 'Eastern Cape', 'gqeberha': 'Eastern Cape',
        'bloemfontein': 'Free State'
    }

    
    def map_province(location):
        if not location:
            return 'Unknown'
        loc = location.lower()
        for city, province in province_map.items():
            if city in loc:
                return province
        return 'Unknown'
    
    df['province'] = df['location'].apply(map_province)
    location = df.drop_duplicates('id').groupby('province')['id'].count().reset_index()
    location.columns = ['province', 'job_count']
    location['snapshot_date'] = today

    return skill_demand, salary, location



def load(skill_demand, salary, location):
    conn = get_connection()
    cursor = conn.cursor()

    # Load skill demand
    for _, row in skill_demand.iterrows():
        cursor.execute("""
            INSERT INTO skill_demand_snapshots (skill, job_count, snapshot_date)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE job_count = VALUES(job_count)
        """, (row['skill'], row['job_count'], row['snapshot_date']))

    # Load salary
    for _, row in salary.iterrows():
        cursor.execute("""
            INSERT INTO salary_snapshots (skill, avg_min, avg_max, snapshot_date)
            VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE avg_min = VALUES(avg_min), avg_max = VALUES(avg_max)
        """, (row['skill'], row['avg_min'], row['avg_max'], row['snapshot_date']))

    # Load location
    for _, row in location.iterrows():
        cursor.execute("""
            INSERT INTO location_snapshots (province, job_count, snapshot_date)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE job_count = VALUES(job_count)
        """, (row['province'], row['job_count'], row['snapshot_date']))

    conn.commit()
    cursor.close()
    conn.close()
    print("Load complete")


if __name__ == "__main__":
    print("Extracting...")
    df = extract()
    print(f"Extracted {len(df)} rows")

    print("Transforming...")
    skill_demand, salary, location = transform(df)
    print(f"Skills: {len(skill_demand)} | Salaries: {len(salary)} | Locations: {len(location)}")

    print("Loading...")
    load(skill_demand, salary, location)
    print("ETL complete")