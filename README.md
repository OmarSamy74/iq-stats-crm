# 📊 IQ Stats CRM

A comprehensive Customer Relationship Management system built with Streamlit, designed for sales teams and management to track leads, deals, and customer interactions.

## 🚀 Features

- **User Management**: Role-based access control (CEO, CTO, Head of Sales, Salesmen)
- **Lead Management**: Complete CRUD operations for leads
- **Deal Tracking**: Track customer deals with payment screenshots
- **Activity Logging**: Comprehensive audit trail for all actions
- **Data Import**: Upload and import leads from Excel/CSV files
- **Analytics Dashboard**: Real-time charts and reports
- **Export Functionality**: Export data to Excel/CSV formats
- **Secure Authentication**: Password hashing with bcrypt

## 👥 User Roles & Access Levels

### Management Team
- **CEO**: ENG Ahmed Essam - Complete system oversight
- **CTO**: Omar Samy - Technical system management
- **Head of Sales**: Mohamed Akmal - Sales team management

### Sales Team
- **Toqa Amin** - Lead management and deal tracking
- **Mahmoud Fathalla** - Lead management and deal tracking
- **Mazen Ashraf** - Lead management and deal tracking
- **Ahmed Malek** - Lead management and deal tracking
- **Youssry Hassan** - Lead management and deal tracking

## 🔐 Security Features

- Secure password hashing using bcrypt
- Role-based access control
- Session management
- Activity logging and audit trails

## 🛠️ Technology Stack

- **Frontend**: Streamlit
- **Backend**: Python, SQLAlchemy
- **Database**: SQLite
- **Charts**: Plotly
- **Authentication**: Passlib with bcrypt
- **Data Processing**: Pandas

## 📦 Installation

1. Clone the repository:
```bash
git clone https://github.com/your-username/iq-stats-crm.git
cd iq-stats-crm
```

2. Create a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Run the application:
```bash
streamlit run main.py
```

## 🌐 Live Demo

Visit the live application: [IQ Stats CRM on streamlit]([https://iq-stats-crm.streamlit.app/])

## 📖 User Manual

For detailed login credentials and usage instructions, see:
- [User Manual](USER_MANUAL.md)
- [Login Credentials](LOGIN_CREDENTIALS.txt)

## 🔧 Configuration

The application uses SQLite as its default database. The database file will be created automatically on first run.

### Environment Variables

No environment variables required for basic setup. The application works out of the box.

## 📊 Database Schema

- **Users**: User accounts with roles and authentication
- **Leads**: Customer leads with status tracking
- **Deals**: Completed deals with payment information
- **Activities**: Audit log of all system activities
- **Comments**: Notes and comments on leads
- **LoginEvents**: Login activity tracking

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📄 License

This project is proprietary software. All rights reserved.

## 📞 Support

For technical support or questions:
- **System Administrator**: Omar Samy (CTO)
- **Sales Support**: Mohamed Akmal (Head of Sales)

---

*© 2024 IQ Stats CRM - All Rights Reserved*
