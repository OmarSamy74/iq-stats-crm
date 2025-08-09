"""
Full-featured Streamlit CRM — single-file example

Features:
- SQLAlchemy ORM models: Users, Leads, Activities (audit log), Comments
- Secure password hashing (passlib) and simple user management (admin can add users)
- Role-based permissions: salesman, head_of_sales, cto, ceo, admin
- Lead lifecycle: statuses (new, contacted, qualified, lost, won), assign to agent
- CRUD for leads: create, read, update, delete (with activity log)
- Upload XLSX/CSV and map columns; bulk-create leads
- Lead detail view with comments (notes) and history
- Filtering, searching, sorting, and pagination
- Dashboards for CTO (time-series, agent breakdown, status breakdown)
- Reporting and export (CSV / Excel) for CEO and admin
- Dockerfile and requirements notes in header

Run (dev):
1. pip install -r requirements.txt
   requirements.txt content below in comment
2. streamlit run streamlit_crm_full.py

Security note: This example uses an in-app user management for demo only.
For production use central auth (OAuth2 / SSO) and secure DB credentials.
"""

import streamlit as st
import pandas as pd
import sqlalchemy as sa
from sqlalchemy.orm import declarative_base, sessionmaker, relationship
from sqlalchemy import Column, Integer, String, DateTime, Text, ForeignKey, LargeBinary, func
from datetime import datetime, date
import io
import plotly.express as px
from passlib.context import CryptContext
import os
import math
import zipfile
import random

# ----------------- Requirements (put in requirements.txt) -----------------
# streamlit
# pandas
# sqlalchemy
# openpyxl
# plotly
# passlib
# python-dateutil
# --------------------------------------------------------------------------

BASE_DIR = os.path.dirname(__file__) if '__file__' in globals() else '.'
DB_FILE = os.path.join(BASE_DIR, 'crm_full.db')
DATABASE_URL = f"sqlite:///{DB_FILE}"

# ----------------- DB setup -----------------
engine = sa.create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# ----------------- Models -----------------
class User(Base):
    __tablename__ = 'users'
    id = Column(Integer, primary_key=True)
    username = Column(String, unique=True, nullable=False)
    name = Column(String)
    password_hash = Column(String, nullable=False)
    role = Column(String, nullable=False)  # salesman, head_of_sales, cto, ceo, admin
    created_at = Column(DateTime, default=datetime.utcnow)

    # relationship to leads via uploaded_by_id
    uploads = relationship('Lead', back_populates='uploader', cascade="all, delete-orphan")
    # relationship to deals via uploaded_by_id
    deals = relationship('Deal', back_populates='uploader', cascade="all, delete-orphan")

    def verify_password(self, password):
        return pwd_context.verify(password, self.password_hash)

    @classmethod
    def hash_password(cls, password):
        return pwd_context.hash(password)

class Lead(Base):
    __tablename__ = 'leads'
    id = Column(Integer, primary_key=True)
    number = Column(String, index=True)
    name = Column(String)
    sales_agent = Column(String, index=True)
    contact = Column(String)
    case_desc = Column(Text)
    feedback = Column(Text)
    status = Column(String, default='new')  # new, contacted, qualified, lost, won
    assigned_to = Column(String, nullable=True)

    # Keep a readable username string and also a foreign-key to users.id
    uploaded_by = Column(String)                     # username (readable)
    uploaded_by_id = Column(Integer, ForeignKey('users.id'))  # FK
    uploaded_at = Column(DateTime, default=datetime.utcnow)

    # relationships
    uploader = relationship('User', back_populates='uploads')
    activities = relationship('Activity', back_populates='lead', cascade="all, delete-orphan")
    comments = relationship('Comment', back_populates='lead', cascade="all, delete-orphan")

class Activity(Base):
    __tablename__ = 'activities'
    id = Column(Integer, primary_key=True)
    lead_id = Column(Integer, ForeignKey('leads.id'))
    actor = Column(String)
    action = Column(String)
    timestamp = Column(DateTime, default=datetime.utcnow)
    detail = Column(Text)
    lead = relationship('Lead', back_populates='activities')

class Comment(Base):
    __tablename__ = 'comments'
    id = Column(Integer, primary_key=True)
    lead_id = Column(Integer, ForeignKey('leads.id'))
    author = Column(String)
    text = Column(Text)
    created_at = Column(DateTime, default=datetime.utcnow)
    lead = relationship('Lead', back_populates='comments')

class Deal(Base):
    __tablename__ = 'deals'
    id = Column(Integer, primary_key=True)
    customer_name = Column(String, nullable=False)
    phone = Column(String, nullable=False)
    payment_screenshot = Column(LargeBinary, nullable=False)
    uploaded_by = Column(String)                     # username (readable)
    uploaded_by_id = Column(Integer, ForeignKey('users.id'))  # FK
    created_at = Column(DateTime, default=datetime.utcnow)
    uploader = relationship('User', back_populates='deals')

class LoginEvent(Base):
    __tablename__ = 'login_events'
    id = Column(Integer, primary_key=True)
    username = Column(String, nullable=False)
    role = Column(String, nullable=False)
    logged_in_at = Column(DateTime, default=datetime.utcnow, index=True)

Base.metadata.create_all(bind=engine)

# Ensure DB schema matches current models (adds new columns if missing)
def ensure_schema():
    """Lightweight auto-migration for SQLite.
    Adds missing columns on existing tables to avoid OperationalError when models evolve.
    """
    with engine.begin() as conn:
        # Leads table columns
        try:
            rows = conn.exec_driver_sql("PRAGMA table_info('leads')").fetchall()
            existing_columns = {row[1] for row in rows}
        except Exception:
            existing_columns = set()

        # Add columns if they don't exist
        if 'uploaded_by' not in existing_columns:
            conn.exec_driver_sql("ALTER TABLE leads ADD COLUMN uploaded_by VARCHAR")
        if 'uploaded_by_id' not in existing_columns:
            conn.exec_driver_sql("ALTER TABLE leads ADD COLUMN uploaded_by_id INTEGER")
        if 'uploaded_at' not in existing_columns:
            conn.exec_driver_sql("ALTER TABLE leads ADD COLUMN uploaded_at DATETIME")
        if 'assigned_to' not in existing_columns:
            conn.exec_driver_sql("ALTER TABLE leads ADD COLUMN assigned_to VARCHAR")
        if 'status' not in existing_columns:
            conn.exec_driver_sql("ALTER TABLE leads ADD COLUMN status VARCHAR DEFAULT 'new'")

        # Optional: ensure indexes exist (SQLite auto-creates pk index; skip others for simplicity)

        # Deals table columns (create table if not exists then add new columns if missing)
        conn.exec_driver_sql("""
        CREATE TABLE IF NOT EXISTS deals (
            id INTEGER PRIMARY KEY,
            customer_name VARCHAR NOT NULL,
            phone VARCHAR NOT NULL,
            payment_screenshot BLOB NOT NULL,
            uploaded_by VARCHAR,
            uploaded_by_id INTEGER,
            created_at DATETIME
        )
        """)
        try:
            rows_deals = conn.exec_driver_sql("PRAGMA table_info('deals')").fetchall()
            existing_deals_cols = {row[1] for row in rows_deals}
        except Exception:
            existing_deals_cols = set()
        if 'uploaded_by' not in existing_deals_cols:
            conn.exec_driver_sql("ALTER TABLE deals ADD COLUMN uploaded_by VARCHAR")
        if 'uploaded_by_id' not in existing_deals_cols:
            conn.exec_driver_sql("ALTER TABLE deals ADD COLUMN uploaded_by_id INTEGER")
        if 'created_at' not in existing_deals_cols:
            conn.exec_driver_sql("ALTER TABLE deals ADD COLUMN created_at DATETIME")

        # Login events table
        conn.exec_driver_sql("""
        CREATE TABLE IF NOT EXISTS login_events (
            id INTEGER PRIMARY KEY,
            username VARCHAR NOT NULL,
            role VARCHAR NOT NULL,
            logged_in_at DATETIME
        )
        """)

ensure_schema()

# ----------------- Utility helpers -----------------

def get_session():
    return SessionLocal()

def get_user_by_username(db, username):
    return db.query(User).filter(User.username == username).first()

def create_user(db, username, password, role='salesman', name=None):
    if get_user_by_username(db, username):
        return None
    user = User(username=username, password_hash=User.hash_password(password), role=role, name=name or username)
    db.add(user)
    db.commit()
    db.refresh(user)
    return user

def update_or_create_user(db, username, password, role='salesman', name=None):
    """Update existing user or create new one"""
    user = get_user_by_username(db, username)
    if user:
        # Update existing user
        user.password_hash = User.hash_password(password)
        user.name = name or user.name
        user.role = role
        db.commit()
        db.refresh(user)
        return user
    else:
        # Create new user
        return create_user(db, username, password, role, name)

def ensure_demo_users():
    db = get_session()
    try:
        # Management roles
        update_or_create_user(db, 'head', 'IQstats@iq-2024', role='head_of_sales', name='Mohamed Akmal')
        update_or_create_user(db, 'cto', 'IQstats@iq-2025', role='cto', name='Omar Samy')
        update_or_create_user(db, 'ceo', 'IQstats@iq-2026', role='ceo', name='ENG Ahmed Essam')
        # Sales team
        update_or_create_user(db, 'toqa', 'IQstats@iq-2027', role='salesman', name='Toqa Amin')
        update_or_create_user(db, 'mahmoud', 'IQstats@iq-2028', role='salesman', name='Mahmoud Fathalla')
        update_or_create_user(db, 'mazen', 'IQstats@iq-2029', role='salesman', name='Mazen Ashraf')
        update_or_create_user(db, 'ahmed_malek', 'IQstats@iq-2030', role='salesman', name='Ahmed Malek')
        update_or_create_user(db, 'youssry', 'IQstats@iq-2031', role='salesman', name='Youssry Hassan')
    finally:
        db.close()

ensure_demo_users()

# ----------------- Activity logger -----------------

def log_activity(db, lead_id, actor, action, detail=None):
    act = Activity(lead_id=lead_id, actor=actor, action=action, detail=detail)
    db.add(act)
    db.commit()

# ----------------- Streamlit UI -----------------

st.set_page_config(page_title='IQ Stats CRM — Full', layout='wide')

# --- Authentication ---
if 'user' not in st.session_state:
    st.session_state['user'] = None

if st.session_state['user'] is None:
    st.title('IQ Stats CRM — Login')
    with st.form('login_form'):
        username = st.text_input('Username')
        password = st.text_input('Password', type='password')
        submitted = st.form_submit_button('Login')
        if submitted:
            db = get_session()
            user = get_user_by_username(db, username)
            if user and user.verify_password(password):
                st.session_state['user'] = user.username
                st.session_state['role'] = user.role
                # log login event
                try:
                    evt = LoginEvent(username=user.username, role=user.role, logged_in_at=datetime.utcnow())
                    db.add(evt)
                    db.commit()
                except Exception:
                    pass
                st.success(f'Welcome, {user.name} ({user.role})')
                st.rerun()
            else:
                st.error('Invalid username or password')


    st.stop()

# Load current user
current_user = None
with get_session() as db:
    current_user = get_user_by_username(db, st.session_state['user'])

if current_user is None:
    st.error('User not found. Please login again.')
    st.session_state.clear()
    st.stop()

role = current_user.role

# Top bar
st.sidebar.title('IQ Stats CRM')
st.sidebar.write(f'Logged in as **{current_user.name}** — *{role}*')
if st.sidebar.button('Logout'):
    for k in list(st.session_state.keys()):
        del st.session_state[k]
    st.rerun()

# Admin: manage users
if role == 'admin':
    st.header('Admin — User Management')
    db = get_session()
    users = db.query(User).all()
    cols = st.columns([1,2,2,1])
    cols[0].write('ID')
    cols[1].write('Username')
    cols[2].write('Name')
    cols[3].write('Role')
    for u in users:
        c0, c1, c2, c3 = st.columns([1,2,2,1])
        c0.write(u.id)
        c1.write(u.username)
        c2.write(u.name)
        c3.write(u.role)

    st.subheader('Create new user')
    with st.form('create_user'):
        new_username = st.text_input('Username')
        new_name = st.text_input('Full name')
        new_pass = st.text_input('Password', type='password')
        new_role = st.selectbox('Role', ['salesman','head_of_sales','cto','ceo','admin'])
        create = st.form_submit_button('Create')
        if create:
            if new_username and new_pass:
                res = create_user(db, new_username, new_pass, role=new_role, name=new_name)
                if res:
                    st.success('User created')
                else:
                    st.error('Username already exists')
            else:
                st.error('Fill username and password')
    db.close()
    st.markdown('---')
    st.subheader('Maintenance')
    if st.button('Reset database (drop & recreate)'):
        try:
            # Close sessions and remove DB file
            try:
                del st.session_state['user']
            except Exception:
                pass
            if os.path.exists(DB_FILE):
                os.remove(DB_FILE)
            # Recreate
            Base.metadata.create_all(bind=engine)
            ensure_schema()
            ensure_demo_users()
            st.success('Database reset. Please reload the app.')
        except Exception as e:
            st.error(f'Failed to reset database: {e}')
    st.stop()

# Common functions for leads

def read_leads_df(filters=None, search=None, order_by='uploaded_at', desc=True, limit=100, offset=0):
    db = get_session()
    q = db.query(Lead)
    if filters:
        for k, v in filters.items():
            if k == 'sales_agent' and v != 'All':
                q = q.filter(Lead.sales_agent == v)
            if k == 'status' and v != 'All':
                q = q.filter(Lead.status == v)
    if search:
        like = f"%{search}%"
        q = q.filter(sa.or_(Lead.name.ilike(like), Lead.number.ilike(like), Lead.contact.ilike(like)))
    total = q.count()
    if order_by:
        col = getattr(Lead, order_by)
        q = q.order_by(col.desc() if desc else col)
    q = q.offset(offset).limit(limit)
    df = pd.read_sql(q.statement, q.session.bind)
    db.close()
    return df, total

def read_deals_df(filters=None, search=None, order_by='created_at', desc=True, limit=1000, offset=0):
    db = get_session()
    q = db.query(Deal)
    if filters:
        for k, v in filters.items():
            if k == 'uploaded_by' and v != 'All':
                q = q.filter(Deal.uploaded_by == v)
    if search:
        like = f"%{search}%"
        q = q.filter(sa.or_(Deal.customer_name.ilike(like), Deal.phone.ilike(like)))
    total = q.count()
    if order_by:
        col = getattr(Deal, order_by)
        q = q.order_by(col.desc() if desc else col)
    q = q.offset(offset).limit(limit)
    # Exclude large binary column when reading to DataFrame
    cols = [Deal.id, Deal.customer_name, Deal.phone, Deal.uploaded_by, Deal.uploaded_by_id, Deal.created_at]
    q = db.query(*cols).filter(Deal.id.in_(db.query(Deal.id).subquery())) if True else q
    df = pd.read_sql(q.statement, q.session.bind)
    db.close()
    return df, total

# ----------------- Export helpers -----------------
def build_deals_excel_with_images(deals):
    """Create an Excel workbook with deals and embedded screenshots if possible.
    Returns bytes, or None if image embedding is unavailable.
    """
    try:
        from openpyxl import Workbook
        from openpyxl.drawing.image import Image as XLImage
    except Exception:
        return None

    wb = Workbook()
    ws = wb.active
    ws.title = 'deals'
    headers = ['id', 'customer_name', 'phone', 'uploaded_by', 'created_at', 'screenshot']
    ws.append(headers)

    # Wider column for screenshot
    try:
        ws.column_dimensions['F'].width = 40
    except Exception:
        pass

    for idx, d in enumerate(deals, start=2):
        ws.cell(row=idx, column=1, value=d.id)
        ws.cell(row=idx, column=2, value=d.customer_name)
        ws.cell(row=idx, column=3, value=d.phone)
        ws.cell(row=idx, column=4, value=d.uploaded_by)
        ws.cell(row=idx, column=5, value=(d.created_at.strftime('%Y-%m-%d %H:%M') if d.created_at else None))
        # Try embedding image
        if getattr(d, 'payment_screenshot', None):
            try:
                img_stream = io.BytesIO(d.payment_screenshot)
                img = XLImage(img_stream)
                cell = f'F{idx}'
                ws.add_image(img, cell)
                # Increase row height for visibility
                try:
                    ws.row_dimensions[idx].height = 120
                except Exception:
                    pass
            except Exception:
                ws.cell(row=idx, column=6, value='[image available]')
        else:
            ws.cell(row=idx, column=6, value='')

    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()

def build_deals_images_zip(deals):
    """Create a zip containing deals' screenshots. Returns bytes."""
    def _detect_ext(content: bytes) -> str:
        # PNG
        if content[:8] == b'\x89PNG\r\n\x1a\n':
            return 'png'
        # JPEG
        if content[:3] == b'\xff\xd8\xff':
            return 'jpg'
        # WEBP (RIFF....WEBP)
        if len(content) >= 12 and content[:4] == b'RIFF' and content[8:12] == b'WEBP':
            return 'webp'
        return 'png'
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, 'w', compression=zipfile.ZIP_DEFLATED) as zf:
        for d in deals:
            content = getattr(d, 'payment_screenshot', None)
            if not content:
                continue
            ext = _detect_ext(content)
            zf.writestr(f'deal_{d.id}_payment.{ext}', content)
    return buf.getvalue()

# Layout by role
if role == 'salesman':
    st.header('Sales')
    tab_leads, tab_deals = st.tabs(['Upload & Manage Leads', 'Done Deals'])
    with tab_leads:
        uploaded_file = st.file_uploader('Upload XLSX/CSV with headers: number, name, sales agent, CONTACT, CASE, FEED BACK', type=['xlsx','xls','csv'])
        # Provide a ready-to-use template
        if st.button('Download Excel template'):
            template = pd.DataFrame({
                'number': [],
                'name': [],
                'sales agent': [],
                'contact': [],
                'case': [],
                'feed back': [],
                'how to contact': [],
            })
            buf = io.BytesIO()
            with pd.ExcelWriter(buf, engine='openpyxl') as writer:
                template.to_excel(writer, index=False, sheet_name='template')
            st.download_button('Download template.xlsx', data=buf.getvalue(), file_name='crm_leads_template.xlsx', mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        if uploaded_file is not None:
            try:
                if uploaded_file.name.endswith('.csv'):
                    df = pd.read_csv(uploaded_file)
                else:
                    df = pd.read_excel(uploaded_file)
            except Exception as e:
                st.error(f'Failed to read file: {e}')
                st.stop()

            st.subheader('Preview')
            st.dataframe(df.head())

        if st.button('Save to CRM'):
            # map columns
            col_map = {c.lower().strip(): c for c in df.columns}
            mapping = {}
            expected = ['number','name','sales agent','contact','case','feed back']
            for k in expected:
                if k in col_map:
                    mapping[col_map[k]] = k
            alt_map = {
                'sales_agent':'sales agent','salesagent':'sales agent',
                'feedback':'feed back','case_desc':'case',
                'contact_number':'contact','phone':'contact',
                'how to contact':'contact','how_to_contact':'contact','contact method':'contact'
            }
            for k,v in alt_map.items():
                if k in col_map and v not in mapping.values():
                    mapping[col_map[k]] = v
            df_ren = df.rename(columns=mapping)
            for tgt in ['number','name','sales agent','contact','case','feed back']:
                if tgt not in df_ren.columns:
                    df_ren[tgt] = None
            leads = []
            db = get_session()
            for _, row in df_ren.iterrows():
                lead = Lead(number=str(row['number']) if row['number'] is not None else None,
                            name=str(row['name']) if row['name'] is not None else None,
                            sales_agent=str(row['sales agent']) if row['sales agent'] is not None else current_user.username,
                            contact=str(row['contact']) if row['contact'] is not None else None,
                            case_desc=str(row['case']) if row['case'] is not None else None,
                            feedback=str(row['feed back']) if row['feed back'] is not None else None,
                            uploaded_by=current_user.username,
                            uploaded_by_id=current_user.id,
                            uploaded_at=datetime.utcnow())
                db.add(lead)
                db.flush()
                log_activity(db, lead.id, current_user.username, 'upload', detail='Bulk upload')
                leads.append(lead)
            db.commit()
            db.close()
            st.success(f'Saved {len(leads)} leads')

        st.markdown('---')
        st.subheader('My Leads')
        page = st.number_input('Page', min_value=1, value=1, key='leads_page')
        page_size = st.selectbox('Page size', [10,25,50], index=0, key='leads_page_size')
        offset = (page-1)*page_size
        df, total = read_leads_df(filters={'sales_agent': current_user.username}, limit=page_size, offset=offset)
        st.write(f'Total: {total}')
        if not df.empty:
            # Prepare editable table
            contact_options = ['call', 'call and whatsapp', "didn't reach"]
            status_options = ['new','contacted','qualified','lost','won']
            case_options = ['general', 'pricing', 'technical', 'support', 'complaint', 'other']
            feedback_options = ['positive', 'neutral', 'negative', 'not interested', 'call later', 'wrong number', 'closed won', 'closed lost', 'other']
            # pull sales users for dropdowns
            with get_session() as _db:
                sales_users = [u.username for u in _db.query(User).filter(User.role=='salesman').all()]
            editable_cols = ['number','name','sales_agent','contact','case_desc','feedback','status','assigned_to','comment_text']
            base_cols = ['id','number','name','sales_agent','contact','case_desc','feedback','status','assigned_to']
            present_cols = [c for c in base_cols if c in df.columns]
            df_edit = df[present_cols].copy()
            # Ensure all expected columns exist in editor
            for c in base_cols:
                if c not in df_edit.columns:
                    df_edit[c] = None
            # UI-only column for creating new comments
            df_edit['comment_text'] = ''
            df_edit['sales_agent'] = df_edit['sales_agent'].fillna(current_user.username)
            df_edit['assigned_to'] = df_edit['assigned_to'].fillna(current_user.username)
            edited = st.data_editor(
                df_edit,
                column_config={
                    'contact': st.column_config.SelectboxColumn('HOW TO CONTACT', options=contact_options, help='How did you contact?'),
                    'status': st.column_config.SelectboxColumn('Status', options=status_options),
                    'sales_agent': st.column_config.SelectboxColumn('Sales Agent', options=sales_users),
                    'assigned_to': st.column_config.SelectboxColumn('Assigned To', options=sales_users),
                    'case_desc': st.column_config.SelectboxColumn('CASE', options=sorted(set(case_options + [c for c in df_edit['case_desc'].dropna().astype(str).tolist()] ))),
                    'feedback': st.column_config.SelectboxColumn('FEED BACK', options=sorted(set(feedback_options + [c for c in df_edit['feedback'].dropna().astype(str).tolist()] ))),
                    'comment_text': st.column_config.TextColumn('Comment (new)'),
                },
                disabled=['id'],
                use_container_width=True,
                key='leads_editor'
            )
            if st.button('Save table changes', key='save_table_changes'):
                import pandas as _pd
                db = get_session()
                try:
                    orig = df_edit.set_index('id')
                    cur = edited.set_index('id', drop=False)
                    # Updates for existing ids
                    intersect_ids = [i for i in cur.index if _pd.notna(i)]
                    for lead_id in intersect_ids:
                        if lead_id not in orig.index:
                            continue
                        row_orig = orig.loc[lead_id]
                        row_new = cur.loc[lead_id]
                        changed = False
                        lead = db.query(Lead).get(int(lead_id))
                        for col in editable_cols:
                            if col == 'comment_text':
                                continue
                            new_val = row_new[col]
                            old_val = row_orig[col]
                            if (isinstance(new_val, float) and _pd.isna(new_val)) or new_val == '':
                                new_val = None
                            if (isinstance(old_val, float) and _pd.isna(old_val)) or old_val == '':
                                old_val = None
                            if new_val != old_val:
                                setattr(lead, col if col != 'case_desc' else 'case_desc', new_val)
                                changed = True
                        if changed:
                            db.add(lead)
                            db.commit()
                            log_activity(db, lead.id, current_user.username, 'edit', detail='Edited via table')
                        # Add a comment if provided
                        comment_text = (row_new.get('comment_text') or '').strip()
                        if comment_text:
                            com = Comment(lead_id=lead.id, author=current_user.username, text=comment_text)
                            db.add(com)
                            db.commit()
                            log_activity(db, lead.id, current_user.username, 'comment', detail=comment_text)
                    # Inserts for new rows (id is NaN)
                    new_rows = edited[_pd.isna(edited['id'])]
                    for _, r in new_rows.iterrows():
                        lead = Lead(
                            number=r.get('number') or None,
                            name=r.get('name') or None,
                            sales_agent=r.get('sales_agent') or current_user.username,
                            contact=r.get('contact') or None,
                            case_desc=r.get('case_desc') or None,
                            feedback=r.get('feedback') or None,
                            status=r.get('status') or 'new',
                            assigned_to=r.get('assigned_to') or None,
                            uploaded_by=current_user.username,
                            uploaded_by_id=current_user.id,
                            uploaded_at=datetime.utcnow()
                        )
                        db.add(lead)
                        db.commit()
                        log_activity(db, lead.id, current_user.username, 'create', detail='Added via table')
                        comment_text = (r.get('comment_text') or '').strip()
                        if comment_text:
                            com = Comment(lead_id=lead.id, author=current_user.username, text=comment_text)
                            db.add(com)
                            db.commit()
                            log_activity(db, lead.id, current_user.username, 'comment', detail=comment_text)
                finally:
                    db.close()
                st.success('Changes saved')
        else:
            st.info('No leads yet')

    with tab_deals:
        st.subheader('Done Deals — Add new')
        with st.form('deal_form'):
            customer_name = st.text_input('Customer name')
            phone = st.text_input('Phone number')
            screenshot_file = st.file_uploader('Payment screenshot (image)', type=['png','jpg','jpeg','webp'])
            submit_deal = st.form_submit_button('Save deal')
            if submit_deal:
                if not customer_name or not phone or not screenshot_file:
                    st.error('Please provide name, phone, and payment screenshot')
                else:
                    try:
                        screenshot_bytes = screenshot_file.read()
                        db = get_session()
                        deal = Deal(
                            customer_name=customer_name,
                            phone=phone,
                            payment_screenshot=screenshot_bytes,
                            uploaded_by=current_user.username,
                            uploaded_by_id=current_user.id,
                        )
                        db.add(deal)
                        db.commit()
                        st.success('Deal saved')
                        db.close()
                    except Exception as e:
                        st.error(f'Failed to save deal: {e}')

        st.subheader('My Deals')
        # Simple list with preview and download
        try:
            db = get_session()
            # Load only metadata first
            deals = db.query(Deal).filter(Deal.uploaded_by==current_user.username).order_by(Deal.created_at.desc()).all()
            for d in deals:
                with st.expander(f"{d.created_at:%Y-%m-%d %H:%M} — {d.customer_name} ({d.phone})"):
                    st.write(f"Salesman: {d.uploaded_by}")
                    # Display image if possible
                    try:
                        st.image(d.payment_screenshot, caption='Payment screenshot', use_container_width=True)
                    except Exception:
                        st.download_button('Download screenshot', data=d.payment_screenshot, file_name=f'deal_{d.id}_payment.png')

            # Downloads: Excel with embedded images (if supported) and ZIP of images
            if deals:
                excel_bytes = build_deals_excel_with_images(deals)
                if excel_bytes:
                    st.download_button('Download my deals (Excel with images)', data=excel_bytes, file_name='my_deals_with_images.xlsx', mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
                zip_bytes = build_deals_images_zip(deals)
                st.download_button('Download my deal screenshots (ZIP)', data=zip_bytes, file_name='my_deal_screenshots.zip', mime='application/zip')
        finally:
            try:
                db.close()
            except Exception:
                pass

elif role == 'head_of_sales':
    st.header('Head of Sales — Overview')
    db = get_session()
    agents = [r[0] for r in db.query(Lead.sales_agent).distinct().all() if r[0]]
    db.close()
    sel_agent = st.selectbox('Filter by agent', options=['All'] + agents)
    sel_status = st.selectbox('Filter by status', options=['All','new','contacted','qualified','lost','won'])
    search = st.text_input('Search (name, number, contact)')
    page = st.number_input('Page', min_value=1, value=1)
    page_size = st.selectbox('Page size', [10,25,50], index=1)
    offset = (page-1)*page_size
    filters = {}
    if sel_agent: filters['sales_agent'] = sel_agent
    if sel_status: filters['status'] = sel_status
    df, total = read_leads_df(filters=filters, search=search, limit=page_size, offset=offset)
    st.write(f'Total matches: {total}')
    st.dataframe(df)
    if not df.empty:
        # quick KPIs
        st.subheader('KPIs')
        c1, c2, c3 = st.columns(3)
        c1.metric('Total Leads', total)
        top_agent = df['sales_agent'].value_counts().idxmax() if not df.empty else '—'
        c2.metric('Top Agent', top_agent)
        c3.metric('Unique Contacts', int(df['contact'].nunique()))

elif role == 'cto':
    st.header('CTO Dashboard — Analytics')
    df_all, total = read_leads_df(limit=10000, offset=0)
    if df_all.empty:
        st.info('No data yet.')
    else:
        # Helpers for categorical charts
        def _normalize_text(value: str):
            if value is None:
                return None
            try:
                text = str(value).strip().lower()
            except Exception:
                return None
            if text in {'', 'na', 'n/a', 'none', 'null', '-'}:
                return None
            return text

        def _plot_top_categories(df: pd.DataFrame, column: str, title: str, top_n: int = 20):
            series = df[column].dropna().astype(str).map(_normalize_text).dropna()
            if series.empty:
                st.info(f'No values in {column} to plot.')
                return
            counts = series.value_counts().head(top_n).reset_index()
            counts.columns = [column, 'count']
            fig_local = px.bar(counts, x=column, y='count', title=title)
            st.plotly_chart(fig_local, use_container_width=True)

        df_all['uploaded_at'] = pd.to_datetime(df_all['uploaded_at'])
        df_all['date'] = df_all['uploaded_at'].dt.date
        st.subheader('Leads over time')
        daily = df_all.groupby('date').size().reset_index(name='count')
        fig = px.line(daily, x='date', y='count', title='Leads per day')
        st.plotly_chart(fig, use_container_width=True)

        # Demo: randomize statuses to see distribution (dev use)
        with st.expander('Demo tools (dev)'):
            if st.button('Randomize lead statuses for demo'):
                with get_session() as db:
                    statuses = ['new','contacted','qualified','lost','won']
                    all_leads = db.query(Lead).all()
                    for lead in all_leads:
                        lead.status = random.choice(statuses)
                    db.commit()
                st.success('Statuses randomized. Refresh charts above.')

        st.subheader('Leads by agent')
        agent_counts = df_all['sales_agent'].value_counts().reset_index()
        agent_counts.columns = ['sales_agent','count']
        fig2 = px.bar(agent_counts, x='sales_agent', y='count', title='Leads by Agent')
        st.plotly_chart(fig2, use_container_width=True)

        # Login events (visibility for CTO)
        st.markdown('---')
        st.subheader('User Login Activity (last 7 days)')
        try:
            with get_session() as db:
                seven_days_ago = datetime.utcnow() - pd.Timedelta(days=7)
                logins = db.query(LoginEvent).filter(LoginEvent.logged_in_at >= seven_days_ago).order_by(LoginEvent.logged_in_at.desc()).all()
                if logins:
                    df_login = pd.DataFrame([
                        {
                            'username': e.username,
                            'role': e.role,
                            'logged_in_at': e.logged_in_at
                        } for e in logins
                    ])
                    st.dataframe(df_login)
                    agg = df_login.groupby('username').size().reset_index(name='logins')
                    fig_login = px.bar(agg, x='username', y='logins', title='Logins per user (7 days)')
                    st.plotly_chart(fig_login, use_container_width=True)
                else:
                    st.info('No login events yet.')
        except Exception:
            st.info('Login activity not available yet.')

        st.subheader('Status breakdown')
        status_counts = df_all['status'].value_counts().reset_index()
        status_counts.columns = ['status','count']
        fig3 = px.pie(status_counts, names='status', values='count', title='Leads by Status')
        st.plotly_chart(fig3, use_container_width=True)

        # Leads by status per salesman (stacked)
        st.subheader('Leads by Status per Salesman')
        status_per_sales = (
            df_all.dropna(subset=['sales_agent','status'])
                  .groupby(['sales_agent','status'])
                  .size()
                  .reset_index(name='count')
        )
        if not status_per_sales.empty:
            fig_status_sales = px.bar(
                status_per_sales,
                x='sales_agent', y='count', color='status',
                barmode='stack', title='Leads by Status per Salesman'
            )
            st.plotly_chart(fig_status_sales, use_container_width=True)
        else:
            st.info('No data for status per salesman yet.')

        st.subheader('Feedback word cloud-ish (top words)')
        feedbacks = df_all['feedback'].dropna().astype(str)
        if not feedbacks.empty:
            all_text = ' '.join(feedbacks.tolist()).lower()
            words = [w.strip('.,!?:;()"\'') for w in all_text.split() if len(w) > 3]
            if words:
                wc = pd.Series(words).value_counts().head(30).reset_index()
                wc.columns = ['word','count']
                fig4 = px.bar(wc, x='word', y='count', title='Top feedback words')
                st.plotly_chart(fig4, use_container_width=True)
            else:
                st.info('No keywords found in feedback yet.')
        else:
            st.info('No feedback text yet to analyze.')

        st.subheader('HOW TO CONTACT — top categories')
        try:
            _plot_top_categories(df_all, 'contact', 'Contact method — top categories')
        except Exception:
            st.info('No values in contact to plot.')

        st.subheader('CASE — top categories')
        try:
            _plot_top_categories(df_all, 'case_desc', 'Case — top categories')
        except Exception:
            st.info('No values in case_desc to plot.')

        st.subheader('FEED BACK — top categories')
        try:
            _plot_top_categories(df_all, 'feedback', 'Feedback — top categories')
        except Exception:
            st.info('No values in feedback to plot.')

        # Done deals charts (handles empty safely)
        st.markdown('---')
        st.subheader('Done Deals — Analytics')
        deals_df, _ = read_deals_df(limit=100000)
        if deals_df.empty:
            st.info('No deals yet')
        else:
            deals_df['created_at'] = pd.to_datetime(deals_df['created_at'])
            deals_df['date'] = deals_df['created_at'].dt.date
            if not deals_df.empty:
                dd_daily = deals_df.groupby('date').size().reset_index(name='deals')
                if not dd_daily.empty:
                    dd_fig = px.line(dd_daily, x='date', y='deals', title='Deals per day')
                    st.plotly_chart(dd_fig, use_container_width=True)
                dd_sales = deals_df['uploaded_by'].value_counts().reset_index()
                dd_sales.columns = ['salesman','deals']
                if not dd_sales.empty:
                    dd_fig2 = px.bar(dd_sales, x='salesman', y='deals', title='Deals by salesman')
                    st.plotly_chart(dd_fig2, use_container_width=True)

elif role == 'ceo':
    st.header('CEO — Reports & Exports')
    df_all, total = read_leads_df(limit=10000, offset=0)
    if df_all.empty:
        st.info('No data yet')
    else:
        st.subheader('Executive KPIs')
        total_leads = total
        unique_contacts = int(df_all['contact'].nunique())
        top_agent = df_all['sales_agent'].value_counts().idxmax() if not df_all.empty else '—'
        c1, c2, c3 = st.columns(3)
        c1.metric('Total Leads', total_leads)
        c2.metric('Unique Contacts', unique_contacts)
        c3.metric('Top Agent', top_agent)

        st.markdown('---')
        st.subheader('Download reports')
        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine='openpyxl') as writer:
            df_all.to_excel(writer, index=False, sheet_name='leads')
        st.download_button('Download Excel report', data=buf.getvalue(), file_name='crm_report.xlsx', mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')

        st.subheader('Summary by agent')
        summary = df_all.groupby('sales_agent').agg(leads=('id','count'), unique_contacts=('contact','nunique')).reset_index()
        st.dataframe(summary)

        # Done Deals Reports
        st.markdown('---')
        st.subheader('Done Deals — Reports & Export')
        deals_df, deals_total = read_deals_df(limit=100000, offset=0)
        if deals_df.empty:
            st.info('No deals submitted yet')
        else:
            deals_df['created_at'] = pd.to_datetime(deals_df['created_at'])
            c1, c2, c3 = st.columns(3)
            c1.metric('Total Deals', deals_total)
            c2.metric('Unique Customers', int(deals_df['customer_name'].nunique()))
            c3.metric('Salesmen Submitting', int(deals_df['uploaded_by'].nunique()))

            # Summaries
            by_salesman = deals_df.groupby('uploaded_by').size().reset_index(name='deals')
            by_day = deals_df.groupby(deals_df['created_at'].dt.date).size().reset_index(name='deals')
            st.write('Deals by Salesman')
            st.dataframe(by_salesman)
            st.write('Deals per Day')
            st.dataframe(by_day)

            # Export comprehensive Excel for advanced analysis
            deals_buf = io.BytesIO()
            with pd.ExcelWriter(deals_buf, engine='openpyxl') as writer:
                deals_df.to_excel(writer, index=False, sheet_name='deals')
                by_salesman.to_excel(writer, index=False, sheet_name='by_salesman')
                by_day.to_excel(writer, index=False, sheet_name='by_day')
            st.download_button('Download Done Deals Excel', data=deals_buf.getvalue(), file_name='done_deals_report.xlsx', mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')

            # Optional: Excel with embedded images for CEO (may be large)
            try:
                db = get_session()
                all_deals = db.query(Deal).order_by(Deal.created_at.desc()).all()
                excel_with_images = build_deals_excel_with_images(all_deals)
                if excel_with_images:
                    st.download_button('Download Done Deals (Excel with images)', data=excel_with_images, file_name='done_deals_with_images.xlsx', mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
                zip_all = build_deals_images_zip(all_deals)
                st.download_button('Download all deal screenshots (ZIP)', data=zip_all, file_name='done_deals_screenshots.zip', mime='application/zip')
            finally:
                try:
                    db.close()
                except Exception:
                    pass

        # Login activity (visibility for CEO)
        st.markdown('---')
        st.subheader('User Login Activity (last 30 days)')
        try:
            with get_session() as db:
                thirty_days_ago = datetime.utcnow() - pd.Timedelta(days=30)
                logins = db.query(LoginEvent).filter(LoginEvent.logged_in_at >= thirty_days_ago).order_by(LoginEvent.logged_in_at.desc()).all()
                if logins:
                    df_login = pd.DataFrame([
                        {
                            'username': e.username,
                            'role': e.role,
                            'logged_in_at': e.logged_in_at
                        } for e in logins
                    ])
                    st.dataframe(df_login)
                    agg = df_login.groupby(['role','username']).size().reset_index(name='logins')
                    fig_login = px.bar(agg, x='username', y='logins', color='role', title='Logins per user (30 days)')
                    st.plotly_chart(fig_login, use_container_width=True)
                else:
                    st.info('No login events yet.')
        except Exception:
            st.info('Login activity not available yet.')

else:
    st.write('Role not supported in UI')

# Footer: activity log quick view for all roles
st.sidebar.markdown('---')
if st.sidebar.checkbox('Show recent activity'):
    db = get_session()
    acts = db.query(Activity).order_by(Activity.timestamp.desc()).limit(50).all()
    for a in acts:
        st.sidebar.write(f"{a.timestamp:%Y-%m-%d %H:%M} — {a.actor} — {a.action} — Lead {a.lead_id}")
    db.close()

# Dockerfile / deployment notes
st.sidebar.markdown('---')
st.sidebar.markdown('**Deployment notes**')
st.sidebar.code('''
# Dockerfile (simple)
FROM python:3.11-slim
WORKDIR /app
COPY . /app
RUN pip install -r requirements.txt
EXPOSE 8501
CMD ["streamlit","run","streamlit_crm_full.py","--server.port","8501","--server.address","0.0.0.0"]
''')

st.sidebar.markdown('Requirements (example)')
st.sidebar.code('''
streamlit
pandas
sqlalchemy
openpyxl
plotly
passlib
python-dateutil
''')

st.sidebar.markdown('Built for demo — I can harden auth, add SSO, multi-tenant support, cloud DB (Postgres), and background workers for emails and integrations. Ask which next!')
