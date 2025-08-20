import streamlit as st
import psycopg2
import pandas as pd
import plotly.graph_objects as go
import re
from datetime import datetime

st.set_page_config(
    page_title="AD Finance - Data Quality Assessment",
    page_icon="📊"
)

# Database connection details
conn_info_mfi = {
    'dbname': 'mfis',
    'user': 'postgres',
    'password': 'CLecaM@%Ejo213',
    'host': '35.177.7.188',
    'port': 5432,
    'connect_timeout': 30
}

# MFI schemas
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

# Columns for tables
cli_columns = [
    'id_client_unique', 'date_adh', 'pp_sexe', 'pp_date_naissance', 'education',
    'id_cpte_base', 'pp_etat_civil', 'num_tel', 'email', 'pp_nationalite',
    'id_loc1', 'province', 'district', 'secteur', 'cellule', 'village',
    'pp_revenu', 'pp_pm_activite_prof', 'langue_correspondance', 'etat'
]
cpt_columns = [
    'id_cpte_unique', 'id_titulaire_unique', 'date_ouvert', 'etat_cpte', 'solde',
    'mode_calcul_int_cpte', 'interet_annuel', 'devise', 'mnt_bloq'
]
dcr_columns = [
    'id_doss_unique', 'id_client_unique', 'id_prod', 'date_dem', 'etat',
    'id_agent_gest', 'cre_etat', 'cre_date_debloc', 'cre_mnt_deb',
    'obj_dem', 'gar_tot'
]
etr_columns = [
    'id_doss_unique', 'id_ech_unique', 'date_ech', 'mnt_cap', 'mnt_int'
]
sre_columns = [
    'id_doss_unique', 'id_ech_unique', 'date_remb', 'mnt_remb_cap', 'mnt_remb_int'
]
mouvement_columns = [
    'id_mouvement_unique', 'id_ecriture_unique', 'compte', 'sens', 'montant', 'devise', 'date_valeur'
]

# Columns per metric
cli_completeness_cols = cli_columns
cli_conformity_cols = ['email', 'pp_date_naissance', 'date_adh']
cli_uniqueness_cols = ['id_client_unique', 'id_cpte_base', 'email', 'num_tel']
cli_validity_cols = ['pp_sexe', 'pp_nationalite', 'pp_etat_civil', 'id_loc1', 'etat', 'pp_revenu']
cli_integrity_cols = ['id_client_unique', 'id_cpte_base', 'email', 'num_tel']

cpt_completeness_cols = cpt_columns
cpt_conformity_cols = ['date_ouvert', 'devise']
cpt_uniqueness_cols = ['id_cpte_unique', 'id_titulaire_unique']
cpt_validity_cols = ['etat_cpte', 'solde', 'interet_annuel', 'mnt_bloq']
cpt_integrity_cols = ['id_cpte_unique', 'id_titulaire_unique']

dcr_completeness_cols = dcr_columns
dcr_conformity_cols = ['date_dem', 'cre_date_debloc']
dcr_uniqueness_cols = ['id_doss_unique', 'id_client_unique']
dcr_validity_cols = ['etat', 'cre_etat', 'cre_mnt_deb', 'gar_tot']
dcr_integrity_cols = ['id_doss_unique', 'id_client_unique']

etr_completeness_cols = etr_columns
etr_conformity_cols = ['date_ech']
etr_uniqueness_cols = ['id_ech_unique']
etr_validity_cols = ['mnt_cap', 'mnt_int']
etr_integrity_cols = ['id_ech_unique', 'id_doss_unique']

sre_completeness_cols = sre_columns
sre_conformity_cols = ['date_remb']
sre_uniqueness_cols = ['id_ech_unique', 'date_remb']
sre_validity_cols = ['mnt_remb_cap', 'mnt_remb_int']
sre_integrity_cols = ['id_ech_unique', 'id_doss_unique']

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

# Streamlit CSS styling
st.markdown("""
<style>
    .block-container {
        padding-top: 0px;
    }
    .stApp > div > div {
        background-color: rgba(0, 0, 0, 0);
        padding: 15px;
        border-radius: 10px;
    }
    .color-legend {
        display: flex;
        align-items: center;
        justify-content: center;
        margin-top: 10px;
    }
    .color-box {
        width: 20px;
        height: 20px;
        display: inline-block;
        vertical-align: middle;
        margin-right: 5px;
    }
    .color-legend span {
        margin-right: 20px;
        font-size: 14px;
    }
    .general-scores-table, .details-table {
        width: 100%;
        border-collapse: collapse;
        margin-top: 20px;
    }
    .general-scores-table th, .general-scores-table td, .details-table th, .details-table td {
        border: 1px solid #ddd;
        padding: 8px;
        text-align: left;
    }
    .general-scores-table th, .details-table th {
        background-color: #f2f2f2;
        color:black;
    }
    .score-good {
        background-color: #008000;
        color: white;
    }
    .score-warning {
        background-color: #FFFF00;
    }
    .score-bad {
        background-color: #FF0000;
        color: white;
    }
</style>
""", unsafe_allow_html=True)

st.title('Data Quality Assessment')

# Initialize session state
if 'show_details' not in st.session_state:
    st.session_state.show_details = False
if 'data_processed' not in st.session_state:
    st.session_state.data_processed = False
if 'metrics' not in st.session_state:
    st.session_state.metrics = []
if 'general_scores' not in st.session_state:
    st.session_state.general_scores = {}
if 'used_columns_markdown' not in st.session_state:
    st.session_state.used_columns_markdown = ""
if 'selected_tables' not in st.session_state:
    st.session_state.selected_tables = []
if 'details_data' not in st.session_state:
    st.session_state.details_data = {}
if 'display_name' not in st.session_state:
    st.session_state.display_name = ""
if 'mfi_choice' not in st.session_state:
    st.session_state.mfi_choice = mfis_schemas[0]
if 'table_choice' not in st.session_state:
    st.session_state.table_choice = list(table_mapping.keys())[0]

# Input selection
left1, right1 = st.columns(2)
with left1:
    mfi_choice = st.selectbox('Select MFI', mfis_schemas, key='mfi_select', index=mfis_schemas.index(st.session_state.mfi_choice))
with right1:
    table_choice = st.selectbox('Select type', list(table_mapping.keys()), key='table_select', index=list(table_mapping.keys()).index(st.session_state.table_choice))

# Submit button
if st.button('Submit'):
    with st.spinner('Processing data please wait...'):
        try:
            # Reset details view and data processed flag
            st.session_state.show_details = False
            st.session_state.data_processed = False
            st.session_state.metrics = []
            st.session_state.general_scores = {}
            st.session_state.used_columns_markdown = ""
            st.session_state.selected_tables = []
            st.session_state.details_data = {}
            st.session_state.display_name = ""

            # Update session state with selections
            st.session_state.mfi_choice = mfi_choice
            st.session_state.table_choice = table_choice
            st.session_state.display_name = table_choice

            conn = psycopg2.connect(**conn_info_mfi)
            cursor = conn.cursor()

            metrics = []
            general_scores = {mfi_choice: []}
            selected_tables = []
            details_data = {}

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
                selected_tables = ['ad_cli']
                details_data['ad_cli'] = {'df': df, 'columns': cli_columns, 'completeness_cols': completeness_cols,
                                         'conformity_cols': conformity_cols, 'uniqueness_cols': uniqueness_cols,
                                         'validity_cols': validity_cols, 'integrity_cols': integrity_cols}
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
                selected_tables = ['ad_cpt']
                details_data['ad_cpt'] = {'df': df, 'columns': cpt_columns, 'completeness_cols': completeness_cols,
                                          'conformity_cols': conformity_cols, 'uniqueness_cols': uniqueness_cols,
                                          'validity_cols': validity_cols, 'integrity_cols': integrity_cols}
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
                            columns = dcr_columns
                        elif table == 'ad_etr':
                            query = f"SELECT {', '.join(etr_columns)} FROM {mfi_choice}.{table}"
                            df = pd.read_sql(query, conn)
                            completeness_cols = etr_completeness_cols
                            conformity_cols = etr_conformity_cols
                            uniqueness_cols = etr_uniqueness_cols
                            validity_cols = etr_validity_cols
                            integrity_cols = etr_integrity_cols
                            columns = etr_columns
                        elif table == 'ad_sre':
                            query = f"SELECT {', '.join(sre_columns)} FROM {mfi_choice}.{table}"
                            df = pd.read_sql(query, conn)
                            completeness_cols = sre_completeness_cols
                            conformity_cols = sre_conformity_cols
                            uniqueness_cols = sre_uniqueness_cols
                            validity_cols = sre_validity_cols
                            integrity_cols = sre_integrity_cols
                            columns = sre_columns

                        if not df.empty:
                            details_data[table] = {'df': df, 'columns': columns, 'completeness_cols': completeness_cols,
                                                  'conformity_cols': conformity_cols, 'uniqueness_cols': uniqueness_cols,
                                                  'validity_cols': validity_cols, 'integrity_cols': integrity_cols}
                            total_rows = len(df)

                            # Completeness
                            completeness = (df[completeness_cols].notnull().mean() * 100).mean()

                            # Conformity
                            def check_conformity(col, data):
                                if col == 'email':
                                    return data[col].str.contains(r'^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$', na=False).mean() * 100
                                elif col in ['pp_date_naissance', 'date_adh', 'date_ouvert', 'date_dem', 'cre_date_debloc', 'date_ech', 'date_remb', 'date_valeur']:
                                    valid_format = data[col].astype(str).str.match(r'^\d{4}-\d{2}-\d{2}$', na=False)
                                    try:
                                        valid_date = pd.to_datetime(data[col], errors='coerce', format='%Y-%m-%d')
                                        not_future = valid_date <= datetime.now()
                                        return (valid_format & valid_date.notnull() & not_future).mean() * 100
                                    except:
                                        return valid_format.mean() * 100
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
                                    st.error(f"Validity check failed for {col}: {str(e)}")
                                    return 0
                            validity = sum(check_validity(col, df) for col in validity_cols) / len(validity_cols)

                            # Integrity
                            unique_non_null = len(df[integrity_cols].drop_duplicates().dropna(how='all')) / total_rows * 100

                            metrics_dict[table] = [completeness, conformity, uniqueness, validity, unique_non_null]
                        else:
                            st.warning(f"Table {table} in {mfi_choice} is empty. Skipping table.")
                    except Exception as e:
                        st.error(f"Error processing {table} in {mfi_choice}: {str(e)}")
                        continue

                if metrics_dict:
                    metrics = [round(sum([metrics_dict[table][i] for table in loan_tables]) / len(loan_tables)) for i in range(5)]
                    general_scores[mfi_choice] = metrics
                    used_columns_markdown = """
                    - **Completeness**: All columns from ad_dcr, ad_etr, ad_sre
                    - **Conformity**: date_dem, cre_date_debloc (ad_dcr); date_ech (ad_etr); date_remb (ad_sre)
                    - **Uniqueness**: id_doss_unique, id_client_unique (ad_dcr); id_ech_unique (ad_etr); id_ech_unique, date_remb (ad_sre)
                    - **Validity**: etat, cre_etat, cre_mnt_deb, gar_tot (ad_dcr); mnt_cap, mnt_int (ad_etr); mnt_remb_cap, mnt_remb_int (ad_sre)
                    - **Integrity**: id_doss_unique, id_client_unique (ad_dcr); id_ech_unique, id_doss_unique (ad_etr, ad_sre)
                    """
                    selected_tables = loan_tables
                else:
                    df = pd.DataFrame()
                    st.error("No valid data retrieved for Loans tables.")
            elif table_choice == 'Transactions':
                try:
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
                        - **Uniqueness**: id_mouvement_unique
                        - **Validity**: sens, montant, devise
                        - **Integrity**: id_mouvement_unique, id_ecriture_unique
                        """
                        selected_tables = ['ad_mouvement']
                        details_data['ad_mouvement'] = {'df': df, 'columns': selected_columns, 'completeness_cols': completeness_cols,
                                                       'conformity_cols': conformity_cols, 'uniqueness_cols': uniqueness_cols,
                                                       'validity_cols': validity_cols, 'integrity_cols': integrity_cols}
                except Exception as e:
                    st.error(f"Error processing {table_mapping[table_choice]} in {mfi_choice}: {str(e)}")
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
                        elif col in ['pp_date_naissance', 'date_adh', 'date_ouvert', 'date_dem', 'cre_date_debloc', 'date_ech', 'date_remb', 'date_valeur']:
                            valid_format = data[col].astype(str).str.match(r'^\d{4}-\d{2}-\d{2}$', na=False)
                            try:
                                valid_date = pd.to_datetime(data[col], errors='coerce', format='%Y-%m-%d')
                                not_future = valid_date <= datetime.now()
                                return (valid_format & valid_date.notnull() & not_future).mean() * 100
                            except:
                                return valid_format.mean() * 100
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
                            st.error(f"Validity check failed for {col}: {str(e)}")
                            return 0
                    validity = round(sum(check_validity(col, df) for col in validity_cols) / len(validity_cols))
                    # Integrity
                    unique_non_null = round(len(df[integrity_cols].drop_duplicates().dropna(how='all')) / total_rows * 100)
                    metrics = [completeness, conformity, uniqueness, validity, unique_non_null]
                    general_scores[mfi_choice] = metrics
                elif table_choice == 'Transactions' and not df.empty:
                    total_rows = len(df)
                    completeness = round((df[completeness_cols].notnull().mean() * 100).mean()) if completeness_cols else 100
                    conformity = round(sum(check_conformity(col, df) for col in conformity_cols) / len(conformity_cols)) if conformity_cols else 100
                    uniqueness = round((df[uniqueness_cols].nunique() / df[uniqueness_cols].notnull().count() * 100).mean()) if uniqueness_cols else 100
                    validity = round(sum(check_validity(col, df) for col in validity_cols) / len(validity_cols)) if validity_cols else 100
                    unique_non_null = round(len(df[integrity_cols].drop_duplicates().dropna(how='all')) / total_rows * 100) if integrity_cols else 100
                    metrics = [completeness, conformity, uniqueness, validity, unique_non_null]
                    general_scores[mfi_choice] = metrics

                st.session_state.metrics = metrics
                st.session_state.general_scores = general_scores
                st.session_state.used_columns_markdown = used_columns_markdown
                st.session_state.selected_tables = selected_tables
                st.session_state.details_data = details_data
                st.session_state.data_processed = True
            else:
                st.error("No valid data retrieved. Please check your selections or database connection.")

        except Exception as e:
            st.error(f"Error during data processing: {str(e)}")
            if 'conn' in locals():
                conn.close()

# Display results if data is processed
if st.session_state.data_processed and st.session_state.metrics and st.session_state.general_scores:
    colors = ['#FF0000' if val < 75 else '#FFFF00' if 75 <= val < 85 else '#008000' for val in st.session_state.metrics]
    fig = go.Figure(data=[
        go.Bar(
            x=['Completeness', 'Conformity', 'Uniqueness', 'Validity', 'Integrity'],
            y=st.session_state.metrics,
            marker_color=colors,
            text=[f'{val:.0f}%' for val in st.session_state.metrics],
            textposition='auto'
        )
    ])
    fig.update_layout(
        title=f'Data Quality Metrics for {st.session_state.display_name} in {st.session_state.mfi_choice}',
        xaxis_title='Metrics',
        yaxis_title='Percentage (%)',
        yaxis=dict(range=[0, 100])
    )

    st.write(f'Dataset: {st.session_state.mfi_choice} {"(ad_dcr, ad_etr, ad_sre)" if st.session_state.table_choice == "Loans" else table_mapping[st.session_state.table_choice]}')
    st.plotly_chart(fig)

    st.markdown("""
    <div class="color-legend">
        <div class="color-box" style="background-color: #008000;"></div><span>85-100% (High quality)</span>
        <div class="color-box" style="background-color: #FFFF00; margin-left: 20px;"></div><span>75-85% (Moderate quality)</span>
        <div class="color-box" style="background-color: #FF0000; margin-left: 20px;"></div><span><75% (Low quality)</span>
    </div>
    """, unsafe_allow_html=True)

    with st.container(border=True):
        right, middle = st.columns(2)
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
            st.markdown(st.session_state.used_columns_markdown)

    # Check Details button
    st.button("Check Details", key="check_details", on_click=lambda: st.session_state.update({'show_details': not st.session_state.show_details}))

# Detailed Metrics Table
if st.session_state.show_details and st.session_state.data_processed and st.session_state.details_data:
    with st.spinner('Generating detailed metrics table...'):
        try:
            for table in st.session_state.selected_tables:
                if table in st.session_state.details_data:
                    st.write(f"Table: {table}")
                    df = st.session_state.details_data[table]['df']
                    columns = st.session_state.details_data[table]['columns']
                    completeness_cols = st.session_state.details_data[table]['completeness_cols']
                    conformity_cols = st.session_state.details_data[table]['conformity_cols']
                    uniqueness_cols = st.session_state.details_data[table]['uniqueness_cols']
                    validity_cols = st.session_state.details_data[table]['validity_cols']
                    integrity_cols = st.session_state.details_data[table]['integrity_cols']
                    total_rows = len(df) if not df.empty else 1

                    details = {col: {'Completeness': 'N/A', 'Conformity': 'N/A', 'Uniqueness': 'N/A',
                                     'Validity': 'N/A', 'Integrity': 'N/A'} for col in columns}

                    for col in completeness_cols:
                        if col in df.columns:
                            details[col]['Completeness'] = round(df[col].notnull().mean() * 100)

                    def check_conformity(col, data):
                        if col == 'email':
                            return round(data[col].str.contains(r'^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$', na=False).mean() * 100)
                        elif col in ['pp_date_naissance', 'date_adh', 'date_ouvert', 'date_dem', 'cre_date_debloc', 'date_ech', 'date_remb', 'date_valeur']:
                            valid_format = data[col].astype(str).str.match(r'^\d{4}-\d{2}-\d{2}$', na=False)
                            try:
                                valid_date = pd.to_datetime(data[col], errors='coerce', format='%Y-%m-%d')
                                not_future = valid_date <= datetime.now()
                                return round((valid_format & valid_date.notnull() & not_future).mean() * 100)
                            except:
                                return round(valid_format.mean() * 100)
                        elif col == 'devise':
                            return round(data[col].str.match(r'^[A-Z]{3}$', na=False).mean() * 100)
                        return 100
                    for col in conformity_cols:
                        if col in df.columns:
                            details[col]['Conformity'] = check_conformity(col, df)

                    for col in uniqueness_cols:
                        if col in df.columns:
                            details[col]['Uniqueness'] = round(df[col].nunique() / df[col].notnull().count() * 100 if df[col].notnull().count() > 0 else 100)

                    def check_validity(col, data):
                        try:
                            if col in ['pp_revenu', 'solde', 'interet_annuel', 'mnt_bloq', 'cre_mnt_deb', 'gar_tot', 'mnt_cap', 'mnt_int', 'mnt_remb_cap', 'mnt_remb_int', 'montant']:
                                return round((pd.to_numeric(data[col], errors='coerce').notnull() & 
                                              (pd.to_numeric(data[col], errors='coerce') >= 0)).mean() * 100)
                            elif col in ['pp_nationalite', 'pp_etat_civil', 'id_loc1', 'etat', 'cre_etat', 'sens', 'devise']:
                                return round(data[col].notnull().mean() * 100)
                            else:
                                return round((pd.to_numeric(data[col], errors='coerce').notnull() & 
                                              (pd.to_numeric(data[col], errors='coerce') >= 0) & 
                                              pd.to_numeric(data[col], errors='coerce').eq(pd.to_numeric(data[col], errors='coerce').astype(int))).mean() * 100)
                        except Exception as e:
                            st.error(f"Validity check failed for {col}: {str(e)}")
                            return 0
                    for col in validity_cols:
                        if col in df.columns:
                            details[col]['Validity'] = check_validity(col, df)

                    if not df.empty:
                        unique_non_null = round(len(df[integrity_cols].drop_duplicates().dropna(how='all')) / total_rows * 100)
                        for col in integrity_cols:
                            details[col]['Integrity'] = unique_non_null

                    table_html = """
                    <table class="details-table">
                        <tr>
                            <th>Column</th>
                            <th>Completeness</th>
                            <th>Conformity</th>
                            <th>Uniqueness</th>
                            <th>Validity</th>
                            <th>Integrity</th>
                        </tr>
                    """
                    for col in columns:
                        table_html += "<tr>"
                        table_html += f"<td>{col}</td>"
                        for metric in ['Completeness', 'Conformity', 'Uniqueness', 'Validity', 'Integrity']:
                            value = details[col][metric]
                            if value == 'N/A':
                                table_html += f"<td>{value}</td>"
                            else:
                                css_class = "score-good" if value >= 85 else "score-warning" if value >= 75 else "score-bad"
                                table_html += f"<td class='{css_class}'>{value}%</td>"
                        table_html += "</tr>"
                    table_html += "</table>"
                    st.markdown(table_html, unsafe_allow_html=True)
                else:
                    st.error(f"No data available for table {table}.")
        except Exception as e:
            st.error(f"Error generating detailed metrics table: {str(e)}")


