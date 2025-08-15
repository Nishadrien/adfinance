import time
import streamlit as st
import psycopg2
import pandas as pd
import plotly.graph_objects as go
import re
from datetime import datetime

# Database connection details for MFIs
conn_info_mfi = {
    'dbname': 'mfis',
    'user': 'postgres',
    'password': 'CLecaM@%Ejo213',
    'host': '35.177.7.188',
    'port': 5432,
    'connect_timeout': 30
}

# List of MFI schemas
mfis_schemas = [
    "mfi_402", "mfi_404", "mfi_406", "mfi_413", "mfi_416",
    "mfi_419", "mfi_421", "mfi_924", "mfi_925", "mfi_926",
    "mfi_934", "mfi_945", "mfi_947", "mfi_956", "mfi_958",
    "mfi_959", "mfi_962", "mfi_963", "mfi_977"
]

# Table mapping
table_mapping = {
    'Clients': 'ad_cli',
    'Accounts': 'ad_cpt',
    'Loans': ['ad_dcr', 'ad_etr', 'ad_sre'],
    'Transactions': 'ad_mouvement'
}

# Columns for ad_cli
cli_columns = [
    'id_client_unique', 'date_adh', 'pp_sexe', 'pp_date_naissance', 'education',
    'id_cpte_base', 'pp_etat_civil', 'num_tel', 'email', 'pp_nationalite',
    'id_loc1', 'province', 'district', 'secteur', 'cellule', 'village',
    'pp_revenu', 'pp_pm_activite_prof', 'langue_correspondance', 'etat'
]

# Columns for ad_cpt
cpt_columns = [
    'id_cpte_unique', 'id_titulaire_unique', 'date_ouvert', 'etat_cpte', 'solde',
    'mode_calcul_int_cpte', 'interet_annuel', 'devise', 'mnt_bloq'
]

# Columns for ad_dcr
dcr_columns = [
    'id_doss_unique', 'id_client_unique', 'id_prod', 'date_dem', 'etat',
    'id_agent_gest', 'cre_etat', 'cre_date_debloc', 'cre_mnt_deb',
    'obj_dem', 'gar_tot'
]

# Columns for ad_etr
etr_columns = [
    'id_doss_unique', 'id_ech_unique', 'date_ech', 'mnt_cap', 'mnt_int'
]

# Columns for ad_sre
sre_columns = [
    'id_doss_unique', 'id_ech_unique', 'date_remb', 'mnt_remb_cap', 'mnt_remb_int'
]

# Columns for ad_mouvement
mouvement_columns = [
    'id_mouvement_unique', 'id_ecriture_unique', 'compte', 'sens', 'montant', 'devise', 'date_valeur'
]

# Columns per metric for ad_cli
cli_completeness_cols = cli_columns
cli_conformity_cols = ['email', 'pp_date_naissance', 'date_adh']
cli_uniqueness_cols = ['id_client_unique', 'id_cpte_base', 'email', 'num_tel']
cli_validity_cols = ['pp_sexe', 'pp_nationalite', 'pp_etat_civil', 'id_loc1', 'etat', 'pp_revenu']
cli_integrity_cols = ['id_client_unique', 'id_cpte_base', 'email', 'num_tel']

# Columns per metric for ad_cpt
cpt_completeness_cols = cpt_columns
cpt_conformity_cols = ['date_ouvert', 'devise']
cpt_uniqueness_cols = ['id_cpte_unique', 'id_titulaire_unique']
cpt_validity_cols = ['etat_cpte', 'solde', 'interet_annuel', 'mnt_bloq']
cpt_integrity_cols = ['id_cpte_unique', 'id_titulaire_unique']

# Columns per metric for ad_dcr
dcr_completeness_cols = dcr_columns
dcr_conformity_cols = ['date_dem', 'cre_date_debloc']
dcr_uniqueness_cols = ['id_doss_unique', 'id_client_unique']
dcr_validity_cols = ['etat', 'cre_etat', 'cre_mnt_deb', 'gar_tot']
dcr_integrity_cols = ['id_doss_unique', 'id_client_unique']

# Columns per metric for ad_etr
etr_completeness_cols = etr_columns
etr_conformity_cols = ['date_ech']
etr_uniqueness_cols = ['id_ech_unique']  # Only id_ech for uniqueness
etr_validity_cols = ['mnt_cap', 'mnt_int']
etr_integrity_cols = ['id_ech_unique', 'id_doss_unique']

# Columns per metric for ad_sre
sre_completeness_cols = sre_columns
sre_conformity_cols = ['date_remb']
sre_uniqueness_cols = ['id_ech_unique', 'date_remb']  # id_ech and date_remb for uniqueness
sre_validity_cols = ['mnt_remb_cap', 'mnt_remb_int']
sre_integrity_cols = ['id_ech_unique', 'id_doss_unique']

# Columns per metric for ad_mouvement
mouvement_completeness_cols = mouvement_columns
mouvement_conformity_cols = ['date_valeur', 'devise']
mouvement_uniqueness_cols = ['id_mouvement_unique']
mouvement_validity_cols = ['sens', 'montant', 'devise']
mouvement_integrity_cols = ['id_mouvement_unique', 'id_ecriture_unique']

# Function to get actual columns from a table
def get_table_columns(cursor, schema, table):
    query = """
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = %s AND table_name = %s
    """
    cursor.execute(query, (schema, table))
    return [row[0] for row in cursor.fetchall()]

# Streamlit app
st.markdown("""
<style>
    .block-container {
        padding-top: 5px;
    }
    .stApp > div > div {
        background-color: rgba(255, 255, 255, 255);
        padding: 20px;
        border-radius: 10px;
    }
</style>
""", unsafe_allow_html=True)

st.title('Data Quality Assessment')

left1, right1 = st.columns(2)
with left1:
    mfi_choice = st.selectbox('Select MFI', mfis_schemas)
with right1:
    table_choice = st.selectbox('Select type', list(table_mapping.keys()))

# Submit button
if st.button('Submit'):
    with st.spinner('Processing please wait...'):
        conn = psycopg2.connect(**conn_info_mfi)
        cursor = conn.cursor()

        # Initialize metrics
        metrics = []
        display_name = table_choice
        used_columns_markdown = ""

        if table_choice == 'Clients':
            query = f"SELECT {', '.join(cli_columns)} FROM {mfi_choice}.{table_mapping[table_choice]} WHERE statut_juridique = 1"
            df = pd.read_sql(query, conn)
            completeness_cols = cli_completeness_cols
            conformity_cols = cli_conformity_cols
            uniqueness_cols = cli_uniqueness_cols
            validity_cols = cli_validity_cols
            integrity_cols = cli_integrity_cols
            used_columns_markdown = """
            - **Completeness**: All columns
            - **Conformity**: email, pp_date_naissance, date_adh
            - **Uniqueness**: id_client_unique, id_cpte_base, email, num_tel
            - **Validity**: pp_sexe, pp_nationalite, pp_etat_civil, id_loc1, etat, pp_revenu
            - **Integrity**: id_client_unique, id_cpte_base, email, num_tel
            """
        elif table_choice == 'Accounts':
            query = f"SELECT {', '.join(cpt_columns)} FROM {mfi_choice}.{table_mapping[table_choice]}"
            df = pd.read_sql(query, conn)
            completeness_cols = cpt_completeness_cols
            conformity_cols = cpt_conformity_cols
            uniqueness_cols = cpt_uniqueness_cols
            validity_cols = cpt_validity_cols
            integrity_cols = cpt_integrity_cols
            used_columns_markdown = """
            - **Completeness**: All columns
            - **Conformity**: date_ouvert, devise
            - **Uniqueness**: id_cpte_unique, id_titulaire_unique
            - **Validity**: etat_cpte, solde, interet_annuel, mnt_bloq
            - **Integrity**: id_cpte_unique, id_titulaire_unique
            """
        elif table_choice == 'Loans':
            loan_tables = table_mapping[table_choice]
            metrics_dict = {}
            for table in loan_tables:
                try:
                    if table == 'ad_dcr':
                        query = f"SELECT {', '.join(dcr_columns)} FROM {mfi_choice}.{table}"
                        df = pd.read_sql(query, conn)
                        completeness_cols = dcr_completeness_cols
                        conformity_cols = dcr_conformity_cols
                        uniqueness_cols = dcr_uniqueness_cols
                        validity_cols = dcr_validity_cols
                        integrity_cols = dcr_integrity_cols
                    elif table == 'ad_etr':
                        query = f"SELECT {', '.join(etr_columns)} FROM {mfi_choice}.{table}"
                        df = pd.read_sql(query, conn)
                        completeness_cols = etr_completeness_cols
                        conformity_cols = etr_conformity_cols
                        uniqueness_cols = etr_uniqueness_cols
                        validity_cols = etr_validity_cols
                        integrity_cols = etr_integrity_cols
                    elif table == 'ad_sre':
                        query = f"SELECT {', '.join(sre_columns)} FROM {mfi_choice}.{table}"
                        df = pd.read_sql(query, conn)
                        completeness_cols = sre_completeness_cols
                        conformity_cols = sre_conformity_cols
                        uniqueness_cols = sre_uniqueness_cols
                        validity_cols = sre_validity_cols
                        integrity_cols = sre_integrity_cols

                    if not df.empty:
                        total_rows = len(df)

                        # Completeness
                        completeness = (df[completeness_cols].notnull().mean() * 100).mean()

                        # Conformity
                        def check_conformity(col, data):
                            if col == 'email':
                                return data[col].str.contains(r'^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$', na=False).mean() * 100
                            elif col in ['pp_date_naissance', 'date_adh']:
                                valid_format = data[col].astype(str).str.match(r'^\d{4}-\d{2}-\d{2}$', na=False)
                                try:
                                    valid_date = pd.to_datetime(data[col], errors='coerce', format='%Y-%m-%d')
                                    not_future = valid_date <= datetime.now()
                                    return (valid_format & valid_date.notnull() & not_future).mean() * 100
                                except:
                                    return valid_format.mean() * 100
                            elif col in ['date_ouvert', 'date_dem', 'cre_date_debloc', 'date_ech', 'date_remb', 'date_valeur']:
                                return data[col].astype(str).str.match(r'^\d{4}-\d{2}-\d{2}$', na=False).mean() * 100
                            elif col == 'devise':
                                return data[col].str.match(r'^[A-Z]{3}$', na=False).mean() * 100
                            return 100
                        conformity = sum(check_conformity(col, df) for col in conformity_cols) / len(conformity_cols) if conformity_cols else 100

                        # Uniqueness
                        uniqueness = (df[uniqueness_cols].nunique() / df[uniqueness_cols].notnull().count() * 100).mean()

                        # Validity
                        def check_validity(col, data):
                            try:
                                if col in ['pp_revenu', 'solde', 'interet_annuel', 'mnt_bloq', 'cre_mnt_deb', 'gar_tot', 'mnt_cap', 'mnt_int', 'mnt_remb_cap', 'mnt_remb_int', 'montant']:
                                    return (pd.to_numeric(data[col], errors='coerce').notnull() & 
                                            (pd.to_numeric(data[col], errors='coerce') >= 0)).mean() * 100
                                elif col in ['pp_nationalite', 'pp_etat_civil', 'id_loc1', 'etat', 'cre_etat', 'sens', 'devise']:
                                    return data[col].notnull().mean() * 100
                                else:
                                    return (pd.to_numeric(data[col], errors='coerce').notnull() & 
                                            (pd.to_numeric(data[col], errors='coerce') >= 0) & 
                                            pd.to_numeric(data[col], errors='coerce').eq(pd.to_numeric(data[col], errors='coerce').astype(int))).mean() * 100
                            except Exception as e:
                                st.write(f"Validity check failed for {col}: {str(e)}")
                                return 0
                        validity = sum(check_validity(col, df) for col in validity_cols) / len(validity_cols)

                        # Integrity
                        unique_non_null = len(df[integrity_cols].drop_duplicates().dropna(how='all')) / total_rows * 100

                        metrics_dict[table] = [completeness, conformity, uniqueness, validity, unique_non_null]
                    else:
                        st.warning(f"Table {table} in {mfi_choice} is empty. Skipping table.")
                except Exception as e:
                    st.warning(f"Error processing {table} in {mfi_choice}: {str(e)}. Skipping table.")
                    continue

            if metrics_dict:
                metrics = [round(sum([metrics_dict[table][i] for table in loan_tables]) / len(loan_tables)) for i in range(5)]
                used_columns_markdown = """
                - **Completeness**: All columns from ad_dcr, ad_etr, ad_sre
                - **Conformity**: date_dem, cre_date_debloc (ad_dcr); date_ech (ad_etr); date_remb (ad_sre)
                - **Uniqueness**: id_doss, id_client (ad_dcr); id_ech (ad_etr); id_ech, date_remb (ad_sre)
                - **Validity**: etat, cre_etat, cre_mnt_deb, gar_tot (ad_dcr); mnt_cap, mnt_int (ad_etr); mnt_remb_cap, mnt_remb_int (ad_sre)
                - **Integrity**: id_doss, id_client (ad_dcr); id_ech, id_doss (ad_etr, ad_sre)
                """
            else:
                df = pd.DataFrame()
                st.error("No valid data retrieved for Loans tables.")
        elif table_choice == 'Transactions':
            try:
                # Dynamically get available columns for ad_mouvement
                available_columns = get_table_columns(cursor, mfi_choice, table_mapping[table_choice])
                selected_columns = [col for col in mouvement_columns if col in available_columns]
                if not selected_columns:
                    st.error(f"No valid columns found for {table_mapping[table_choice]} in {mfi_choice}.")
                    df = pd.DataFrame()
                else:
                    query = f"SELECT {', '.join(selected_columns)} FROM {mfi_choice}.{table_mapping[table_choice]}"
                    df = pd.read_sql(query, conn)
                    completeness_cols = [col for col in mouvement_completeness_cols if col in selected_columns]
                    conformity_cols = [col for col in mouvement_conformity_cols if col in selected_columns]
                    uniqueness_cols = [col for col in mouvement_uniqueness_cols if col in selected_columns]
                    validity_cols = [col for col in mouvement_validity_cols if col in selected_columns]
                    integrity_cols = [col for col in mouvement_integrity_cols if col in selected_columns]
                    used_columns_markdown = """
                    - **Completeness**: All columns
                    - **Conformity**: date_valeur, devise
                    - **Uniqueness**: id_mouvement
                    - **Validity**: sens, montant, devise
                    - **Integrity**: id_mouvement, id_ecriture
                    """
            except Exception as e:
                st.error(f"Error processing {table_mapping[table_choice]} in {mfi_choice}: {str(e)}.")
                df = pd.DataFrame()

        else:
            st.error("Invalid table choice selected.")
            df = pd.DataFrame()

        cursor.close()
        conn.close()

        if not df.empty or (table_choice == 'Loans' and metrics_dict):
            if table_choice not in ['Loans', 'Transactions']:
                total_rows = len(df)

                # Completeness
                completeness = round((df[completeness_cols].notnull().mean() * 100).mean())

                # Conformity
                def check_conformity(col, data):
                    if col == 'email':
                        return data[col].str.contains(r'^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$', na=False).mean() * 100
                    elif col in ['pp_date_naissance', 'date_adh']:
                        valid_format = data[col].astype(str).str.match(r'^\d{4}-\d{2}-\d{2}$', na=False)
                        try:
                            valid_date = pd.to_datetime(data[col], errors='coerce', format='%Y-%m-%d')
                            not_future = valid_date <= datetime.now()
                            return (valid_format & valid_date.notnull() & not_future).mean() * 100
                        except:
                            return valid_format.mean() * 100
                    elif col in ['date_ouvert', 'date_dem', 'cre_date_debloc', 'date_ech', 'date_remb', 'date_valeur']:
                        return data[col].astype(str).str.match(r'^\d{4}-\d{2}-\d{2}$', na=False).mean() * 100
                    elif col == 'devise':
                        return data[col].str.match(r'^[A-Z]{3}$', na=False).mean() * 100
                    return 100
                conformity = round(sum(check_conformity(col, df) for col in conformity_cols) / len(conformity_cols)) if conformity_cols else 100

                # Uniqueness
                uniqueness = round((df[uniqueness_cols].nunique() / df[uniqueness_cols].notnull().count() * 100).mean())

                # Validity
                def check_validity(col, data):
                    try:
                        if col in ['pp_revenu', 'solde', 'interet_annuel', 'mnt_bloq', 'cre_mnt_deb', 'gar_tot', 'mnt_cap', 'mnt_int', 'mnt_remb_cap', 'mnt_remb_int', 'montant']:
                            return (pd.to_numeric(data[col], errors='coerce').notnull() & 
                                    (pd.to_numeric(data[col], errors='coerce') >= 0)).mean() * 100
                        elif col in ['pp_nationalite', 'pp_etat_civil', 'id_loc1', 'etat', 'cre_etat', 'sens', 'devise']:
                            return data[col].notnull().mean() * 100
                        else:
                            return (pd.to_numeric(data[col], errors='coerce').notnull() & 
                                    (pd.to_numeric(data[col], errors='coerce') >= 0) & 
                                    pd.to_numeric(data[col], errors='coerce').eq(pd.to_numeric(data[col], errors='coerce').astype(int))).mean() * 100
                    except Exception as e:
                        st.write(f"Validity check failed for {col}: {str(e)}")
                        return 0
                validity = round(sum(check_validity(col, df) for col in validity_cols) / len(validity_cols))

                # Integrity
                unique_non_null = round(len(df[integrity_cols].drop_duplicates().dropna(how='all')) / total_rows * 100)

                metrics = [completeness, conformity, uniqueness, validity, unique_non_null]
            elif table_choice == 'Transactions' and not df.empty:
                total_rows = len(df)

                # Completeness
                completeness = round((df[completeness_cols].notnull().mean() * 100).mean()) if completeness_cols else 100

                # Conformity
                def check_conformity(col, data):
                    if col == 'email':
                        return data[col].str.contains(r'^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$', na=False).mean() * 100
                    elif col in ['pp_date_naissance', 'date_adh']:
                        valid_format = data[col].astype(str).str.match(r'^\d{4}-\d{2}-\d{2}$', na=False)
                        try:
                            valid_date = pd.to_datetime(data[col], errors='coerce', format='%Y-%m-%d')
                            not_future = valid_date <= datetime.now()
                            return (valid_format & valid_date.notnull() & not_future).mean() * 100
                        except:
                            return valid_format.mean() * 100
                    elif col in ['date_ouvert', 'date_dem', 'cre_date_debloc', 'date_ech', 'date_remb', 'date_valeur']:
                        return data[col].astype(str).str.match(r'^\d{4}-\d{2}-\d{2}$', na=False).mean() * 100
                    elif col == 'devise':
                        return data[col].str.match(r'^[A-Z]{3}$', na=False).mean() * 100
                    return 100
                conformity = round(sum(check_conformity(col, df) for col in conformity_cols) / len(conformity_cols)) if conformity_cols else 100

                # Uniqueness
                uniqueness = round((df[uniqueness_cols].nunique() / df[uniqueness_cols].notnull().count() * 100).mean()) if uniqueness_cols else 100

                # Validity
                def check_validity(col, data):
                    try:
                        if col in ['pp_revenu', 'solde', 'interet_annuel', 'mnt_bloq', 'cre_mnt_deb', 'gar_tot', 'mnt_cap', 'mnt_int', 'mnt_remb_cap', 'mnt_remb_int', 'montant']:
                            return (pd.to_numeric(data[col], errors='coerce').notnull() & 
                                    (pd.to_numeric(data[col], errors='coerce') >= 0)).mean() * 100
                        elif col in ['pp_nationalite', 'pp_etat_civil', 'id_loc1', 'etat', 'cre_etat', 'sens', 'devise']:
                            return data[col].notnull().mean() * 100
                        else:
                            return (pd.to_numeric(data[col], errors='coerce').notnull() & 
                                    (pd.to_numeric(data[col], errors='coerce') >= 0) & 
                                    pd.to_numeric(data[col], errors='coerce').eq(pd.to_numeric(data[col], errors='coerce').astype(int))).mean() * 100
                    except Exception as e:
                        st.write(f"Validity check failed for {col}: {str(e)}")
                        return 0
                validity = round(sum(check_validity(col, df) for col in validity_cols) / len(validity_cols)) if validity_cols else 100

                # Integrity
                unique_non_null = round(len(df[integrity_cols].drop_duplicates().dropna(how='all')) / total_rows * 100) if integrity_cols else 100

                metrics = [completeness, conformity, uniqueness, validity, unique_non_null]
            elif table_choice == 'Transactions' and df.empty:
                st.error("No valid data retrieved for Transactions table.")
                metrics = []

            # Define colors based on percentage
            colors = ['#8B0000' if val < 60 else 'yellow' if 60 <= val < 90 else '#36A2EB' for val in metrics]

            # Create bar chart
            fig = go.Figure(data=[
                go.Bar(
                    x=['Completeness', 'Conformity', 'Uniqueness', 'Validity', 'Integrity'],
                    y=metrics,
                    marker_color=colors,
                    text=[f'{val:.0f}%' for val in metrics],
                    textposition='auto'
                )
            ])
            fig.update_layout(
                title=f'Data Quality Metrics for {display_name} in {mfi_choice}',
                xaxis_title='Metrics',
                yaxis_title='Percentage (%)',
                yaxis=dict(range=[0, 100])
            )

            # Display chart and metrics
            st.write(f'Dataset: {mfi_choice} {"(ad_dcr, ad_etr, ad_sre)" if table_choice == "Loans" else table_mapping[table_choice]}')
            st.plotly_chart(fig)

            with st.container(border=True):
                left, right, middle = st.columns(3)
                with left:
                    st.subheader('Metrics Summary')
                    st.write(f'- Completeness: {metrics[0]:.0f}% (Non-missing rows)')
                    st.write(f'- Conformity: {metrics[1]:.0f}% (Valid format per field)')
                    st.write(f'- Uniqueness: {metrics[2]:.0f}% (Non-duplicate values)')
                    st.write(f'- Validity: {metrics[3]:.0f}% (Valid values)')
                    st.write(f'- Integrity: {metrics[4]:.0f}% (Unique, non-null records)')
                with right:
                    st.subheader('Definitions')
                    st.markdown("""
                    - **Completeness**: Percentage of non-missing rows.
                    - **Conformity**: Percentage of fields with valid format/standardization.
                    - **Uniqueness**: Percentage of non-duplicate values.
                    - **Validity**: Percentage of accurate/correct values.
                    - **Integrity**: Percentage of unique, non-null records.
                    """)
                with middle:
                    st.subheader('Used Columns')
                    st.markdown(used_columns_markdown)
            st.page_link("https://cenfriglobal.sharepoint.com/:x:/r/sites/ClientMastercardFoundation/Shared%20Documents/2.%20REDP2/08.%20Data%20Hub/MCFD2412%20-%20AD%20Finance%20analytics/Analytical%20framework/Final%20Version/AD%20Finance%20Analytical%20framework_with%20tables_columns.xlsb.xlsx?d=w23c7e5eeb11a4cf891be379f35562294&csf=1&web=1&e=zZ2yvq", label="Check Details", icon="🗒️")

                    




