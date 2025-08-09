# CTO Lead Archiving Features

## Overview
The CTO role now has comprehensive lead archiving capabilities that allow for efficient management of leads from salesmen, including archiving, unarchiving, and date-based organization.

## New Features Added

### 1. Lead Archiving Fields
- `is_archived`: Status field (yes/no)
- `archived_by`: Username of who archived the lead
- `archived_at`: Timestamp when archived
- `archive_reason`: Reason for archiving
- `archive_date`: Scheduled archive date

### 2. CTO Archiving Interface

#### Archive Leads Tab
- **Filter leads** by sales agent, status, and search terms
- **Select multiple leads** for bulk archiving
- **Choose archive reasons** from predefined options or add custom reasons
- **Set archive dates** for scheduling
- **Archive selected leads** with detailed logging

#### View Archived Tab
- **View all archived leads** with archive information
- **Unarchive leads** individually or in bulk
- **Export archived leads** by date range in Excel or CSV format
- **Detailed archive information** including reasons and timestamps

#### Bulk Archive Tab
- **Archive by criteria**: sales agent, status, age
- **Archive old leads** automatically (e.g., older than 30 days)
- **Bulk archive reasons** for system cleanup
- **Efficient bulk operations** with progress tracking

#### Archive by Date Tab
- **Archive leads by date range** (e.g., specific period)
- **View archived leads by specific date**
- **Date-based cleanup** for system maintenance
- **Historical archive tracking**

### 3. Manual Lead Creation
- **Add new leads manually** through CTO interface
- **Assign to sales agents** directly
- **Set initial status** and other details
- **Immediate availability** for sales team

### 4. Archive Summary Dashboard
- **Real-time metrics**: active vs archived leads
- **Archive rate calculations**
- **Archive trends** over time
- **Archive reasons breakdown**
- **Archive by agent statistics**

### 5. Export Functionality
- **Comprehensive reports** with all archive details
- **Date range filtering** for exports
- **Excel and CSV formats**
- **Summary statistics** included in exports

## Usage Instructions

### For CTO Users:

1. **Login as CTO** (username: `cto`, password: `IQstats@iq-2025`)

2. **Access Archiving Features**:
   - Navigate to the "📦 CTO Lead Archiving Management" section
   - Use the four tabs for different archiving functions

3. **Archive Individual Leads**:
   - Go to "Archive Leads" tab
   - Filter leads as needed
   - Select leads to archive
   - Choose reason and date
   - Click "🗄️ Archive Selected Leads"

4. **View and Manage Archived**:
   - Go to "View Archived" tab
   - View all archived leads
   - Unarchive if needed
   - Export reports

5. **Bulk Operations**:
   - Use "Bulk Archive" for criteria-based archiving
   - Use "Archive by Date" for date-based operations

6. **Add New Leads**:
   - Use "Add New Leads Manually (CTO)" expander
   - Fill in lead details
   - Assign to sales agent
   - Click "➕ Add New Lead"

### Archive Reasons Available:
- Completed/Closed
- No longer relevant
- Duplicate lead
- Wrong information
- Customer request
- System cleanup
- Other (with custom text)

### Export Features:
- **Date range selection** for exports
- **Excel format** with multiple sheets
- **CSV format** for data analysis
- **Summary statistics** included
- **Detailed archive information** with timestamps

## Technical Implementation

### Database Changes:
- Added 5 new columns to `leads` table
- Automatic schema migration for existing databases
- Backward compatibility maintained

### New Functions:
- `archive_lead()`: Archive individual lead
- `unarchive_lead()`: Unarchive individual lead
- `bulk_archive_leads()`: Bulk archive operation
- `get_archived_leads_by_date()`: Date-based queries
- `export_archived_leads_report()`: Export functionality

### Security:
- Only CTO role can access archiving features
- All operations logged in activity history
- Audit trail maintained for all archive/unarchive actions

## Benefits

1. **Better Organization**: Separate active and archived leads
2. **Improved Performance**: Reduce clutter in active lead lists
3. **Historical Tracking**: Maintain complete archive history
4. **Flexible Management**: Multiple ways to archive and organize
5. **Reporting**: Comprehensive export capabilities
6. **Audit Trail**: Complete logging of all archive operations

## Date-Based Features

The system now supports:
- **Archive by specific dates**
- **View archives by date**
- **Export by date ranges**
- **Scheduled archiving**
- **Historical archive tracking**

This makes it easy to organize leads by day and date as requested, providing full visibility into when leads were archived and by whom. 