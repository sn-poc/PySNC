"""
ServiceNow Fluent Code Examples using PySNC

Demonstrates the GlideRecord-style fluent API for common ServiceNow operations:
querying, filtering, CRUD, attachments, batching, serialization, and async usage.

Usage:
    export PYSNC_INSTANCE='https://yourinstance.service-now.com'
    export PYSNC_USER='admin'
    export PYSNC_PASSWORD='password'
    python examples/fluent_examples.py
"""

import os
import sys

from pysnc import ServiceNowClient


# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------

def get_client() -> ServiceNowClient:
    """Create a ServiceNowClient from environment variables."""
    instance = os.environ.get("PYSNC_INSTANCE", "https://dev00000.service-now.com")
    user = os.environ.get("PYSNC_USER", "admin")
    password = os.environ.get("PYSNC_PASSWORD", "")
    return ServiceNowClient(instance, (user, password))


# ---------------------------------------------------------------------------
# 1. Basic Query & Iteration
# ---------------------------------------------------------------------------

def query_active_incidents(client: ServiceNowClient):
    """Query active incidents, limiting to 5 results with specific fields."""
    gr = client.GlideRecord('incident')
    gr.add_active_query()
    gr.fields = ['number', 'short_description', 'priority', 'state', 'assigned_to']
    gr.limit = 5
    gr.order_by('number')
    gr.query()

    print(f"Total active incidents: {gr.get_row_count()}")
    for record in gr:
        print(f"  {record.number} | {record.short_description} | Priority: {record.priority.get_display_value()}")


# ---------------------------------------------------------------------------
# 2. Get a Single Record by sys_id
# ---------------------------------------------------------------------------

def get_user_by_sys_id(client: ServiceNowClient, sys_id: str):
    """Retrieve a single user record by sys_id."""
    gr = client.GlideRecord('sys_user')
    if gr.get(sys_id):
        print(f"Found user: {gr.get_display_value('name')} ({gr.get_value('user_name')})")
        print(f"  Email: {gr.get_value('email')}")
        print(f"  Active: {gr.get_value('active')}")
        print(f"  Link: {gr.get_link(no_stack=True)}")
    else:
        print(f"No user found with sys_id: {sys_id}")


# ---------------------------------------------------------------------------
# 3. Get a Single Record by Field Value
# ---------------------------------------------------------------------------

def get_user_by_username(client: ServiceNowClient, username: str):
    """Retrieve a user by user_name field."""
    gr = client.GlideRecord('sys_user')
    if gr.get('user_name', username):
        print(f"Found: {gr.get_display_value('name')} (sys_id: {gr.sys_id})")
    else:
        print(f"No user found: {username}")


# ---------------------------------------------------------------------------
# 4. Complex Query with Operators
# ---------------------------------------------------------------------------

def query_high_priority_incidents(client: ServiceNowClient):
    """Query incidents with priority 1 or 2 created in the last 30 days."""
    gr = client.GlideRecord('incident')
    gr.add_active_query()

    # OR condition: priority is 1 OR 2
    priority_query = gr.add_query('priority', '1')
    priority_query.add_or_condition('priority', '2')

    # String operator: short_description contains 'error'
    gr.add_query('short_description', 'CONTAINS', 'error')

    gr.fields = ['number', 'short_description', 'priority', 'sys_created_on']
    gr.order_by_desc('sys_created_on')
    gr.limit = 10
    gr.query()

    print(f"High priority incidents containing 'error': {gr.get_row_count()}")
    for record in gr:
        print(f"  {record.number} | P{record.priority} | {record.short_description}")


# ---------------------------------------------------------------------------
# 5. Encoded Query
# ---------------------------------------------------------------------------

def query_with_encoded_query(client: ServiceNowClient):
    """Use a raw encoded query string (same as sysparm_query)."""
    gr = client.GlideRecord('incident')
    gr.add_encoded_query('active=true^priority=1^stateNOT IN6,7')
    gr.fields = ['number', 'short_description', 'state']
    gr.limit = 5
    gr.query()

    print(f"Encoded query results: {gr.get_row_count()}")
    for record in gr:
        print(f"  {record.number} | {record.short_description}")


# ---------------------------------------------------------------------------
# 6. Null / Not-Null Queries
# ---------------------------------------------------------------------------

def query_unassigned_incidents(client: ServiceNowClient):
    """Find active incidents with no assigned_to."""
    gr = client.GlideRecord('incident')
    gr.add_active_query()
    gr.add_null_query('assigned_to')
    gr.fields = ['number', 'short_description', 'priority']
    gr.limit = 5
    gr.query()

    print(f"Unassigned incidents: {gr.get_row_count()}")
    for record in gr:
        print(f"  {record.number} | {record.short_description}")


# ---------------------------------------------------------------------------
# 7. Join Query
# ---------------------------------------------------------------------------

def query_users_who_manage_groups(client: ServiceNowClient):
    """Find users who are managers of active groups using a join query."""
    gr = client.GlideRecord('sys_user')
    join_query = gr.add_join_query('sys_user_group', join_table_field='manager')
    join_query.add_query('active', 'true')
    gr.fields = ['user_name', 'name', 'email']
    gr.limit = 10
    gr.query()

    print(f"Users who manage active groups: {gr.get_row_count()}")
    for record in gr:
        print(f"  {record.get_display_value('name')} ({record.user_name})")


# ---------------------------------------------------------------------------
# 8. Related List (RL) Query
# ---------------------------------------------------------------------------

def query_users_with_admin_role(client: ServiceNowClient):
    """Find users who have the admin role via RL query."""
    gr = client.GlideRecord('sys_user')
    rl = gr.add_rl_query('sys_user_has_role', 'user', '>0', stop_at_relationship=True)
    rl.add_query('role.name', 'admin')
    gr.fields = ['user_name', 'name']
    gr.limit = 10
    gr.query()

    print(f"Users with admin role: {gr.get_row_count()}")
    for record in gr:
        print(f"  {record.get_display_value('name')} ({record.user_name})")


# ---------------------------------------------------------------------------
# 9. Display Values
# ---------------------------------------------------------------------------

def demonstrate_display_values(client: ServiceNowClient):
    """Show the difference between value and display_value."""
    gr = client.GlideRecord('incident')
    gr.add_active_query()
    gr.fields = ['number', 'assigned_to', 'priority', 'state']
    gr.limit = 3
    gr.query()

    for record in gr:
        print(f"Incident: {record.number}")
        print(f"  assigned_to value:   {record.get_value('assigned_to')}")
        print(f"  assigned_to display: {record.get_display_value('assigned_to')}")
        print(f"  priority value:      {record.get_value('priority')}")
        print(f"  priority display:    {record.get_display_value('priority')}")
        print(f"  state value:         {record.get_value('state')}")
        print(f"  state display:       {record.get_display_value('state')}")
        print()


# ---------------------------------------------------------------------------
# 10. GlideElement Access (Dot-Walk Style)
# ---------------------------------------------------------------------------

def demonstrate_glide_element(client: ServiceNowClient):
    """Access field elements directly via attribute access."""
    gr = client.GlideRecord('incident')
    gr.fields = ['number', 'short_description', 'caller_id', 'caller_id.email']
    gr.limit = 3
    gr.query()

    for record in gr:
        # Direct attribute access returns a GlideElement
        number_element = record.number
        print(f"Number: {number_element}")
        print(f"  Element type: {type(number_element).__name__}")
        print(f"  Value: {number_element.get_value()}")
        print(f"  Display: {number_element.get_display_value()}")

        # Dot-walk: access related record fields
        caller_email = record.get_value('caller_id.email')
        if caller_email:
            print(f"  Caller email: {caller_email}")
        print()


# ---------------------------------------------------------------------------
# 11. Insert a Record
# ---------------------------------------------------------------------------

def insert_incident(client: ServiceNowClient) -> str:
    """Create a new incident record."""
    gr = client.GlideRecord('incident')
    gr.initialize()
    gr.set_value('short_description', 'PySNC Example: Test incident')
    gr.set_value('description', 'Created via PySNC fluent API example')
    gr.set_value('priority', '3')
    gr.set_value('urgency', '3')
    gr.set_value('impact', '3')

    sys_id = gr.insert()
    if sys_id:
        print(f"Created incident: {gr.number} (sys_id: {sys_id})")
        return str(sys_id)
    else:
        print("Failed to create incident")
        return ""


# ---------------------------------------------------------------------------
# 12. Update a Record
# ---------------------------------------------------------------------------

def update_incident(client: ServiceNowClient, sys_id: str):
    """Update an existing incident."""
    gr = client.GlideRecord('incident')
    if gr.get(sys_id):
        gr.set_value('short_description', 'PySNC Example: Updated incident')
        gr.set_value('work_notes', 'Updated via PySNC fluent API')
        result = gr.update()
        if result:
            print(f"Updated incident: {gr.number}")
        else:
            print("Update failed")
    else:
        print(f"Incident not found: {sys_id}")


# ---------------------------------------------------------------------------
# 13. Update using attribute-style assignment
# ---------------------------------------------------------------------------

def update_with_attribute_style(client: ServiceNowClient, sys_id: str):
    """Update fields using Pythonic attribute assignment."""
    gr = client.GlideRecord('incident')
    if gr.get(sys_id):
        # Attribute-style assignment (calls set_value under the hood)
        gr.short_description = 'PySNC Example: Attribute-style update'
        gr.work_notes = 'Updated using gr.field = value syntax'

        if gr.changes():
            gr.update()
            print(f"Updated incident via attribute style: {gr.number}")
    else:
        print(f"Incident not found: {sys_id}")


# ---------------------------------------------------------------------------
# 14. Delete a Record
# ---------------------------------------------------------------------------

def delete_incident(client: ServiceNowClient, sys_id: str):
    """Delete a single incident by sys_id."""
    gr = client.GlideRecord('incident')
    if gr.get(sys_id):
        number = str(gr.number)
        if gr.delete():
            print(f"Deleted incident: {number}")
    else:
        print(f"Incident not found: {sys_id}")


# ---------------------------------------------------------------------------
# 15. Batch Delete (delete_multiple)
# ---------------------------------------------------------------------------

def delete_test_incidents(client: ServiceNowClient):
    """Delete all incidents matching a query using delete_multiple."""
    gr = client.GlideRecord('incident')
    gr.add_query('short_description', 'STARTSWITH', 'PySNC Example:')
    gr.fields = ['sys_id']
    result = gr.delete_multiple()
    print(f"Batch delete {'succeeded' if result else 'had failures'}")


# ---------------------------------------------------------------------------
# 16. Batch Update (update_multiple)
# ---------------------------------------------------------------------------

def batch_update_priority(client: ServiceNowClient):
    """Update the priority of multiple incidents at once."""
    gr = client.GlideRecord('incident')
    gr.add_query('short_description', 'STARTSWITH', 'PySNC Example:')
    gr.fields = ['sys_id', 'priority', 'short_description']
    gr.query()

    for record in gr:
        record.set_value('priority', '4')

    result = gr.update_multiple()
    print(f"Batch update {'succeeded' if result else 'had failures'}")


# ---------------------------------------------------------------------------
# 17. Serialization
# ---------------------------------------------------------------------------

def demonstrate_serialization(client: ServiceNowClient):
    """Serialize records to dicts."""
    gr = client.GlideRecord('incident')
    gr.add_active_query()
    gr.fields = ['number', 'short_description', 'priority']
    gr.limit = 3
    gr.query()

    # Serialize current record (values only)
    if gr.next():
        values_only = gr.serialize(display_value=False)
        print(f"Values only: {values_only}")

        display_only = gr.serialize(display_value=True)
        print(f"Display only: {display_only}")

        both = gr.serialize(display_value='both')
        print(f"Both: {both}")

    # Serialize all records
    gr.rewind()
    all_records = gr.serialize_all(display_value=False)
    print(f"\nAll records ({len(all_records)}):")
    for rec in all_records:
        print(f"  {rec}")


# ---------------------------------------------------------------------------
# 18. Pandas Integration
# ---------------------------------------------------------------------------

def demonstrate_pandas_export(client: ServiceNowClient):
    """Export query results to a pandas-compatible dict."""
    gr = client.GlideRecord('sys_user')
    gr.add_active_query()
    gr.fields = ['user_name', 'name', 'email', 'department']
    gr.limit = 5
    gr.query()

    # to_pandas returns an OrderedDict suitable for pd.DataFrame()
    data = gr.to_pandas(mode='smart')
    print("Pandas export columns:", list(data.keys()))
    for col, values in data.items():
        print(f"  {col}: {values}")

    # With explicit mode='value' or mode='display'
    gr.rewind()
    value_data = gr.to_pandas(mode='value')
    print("\nValue-only columns:", list(value_data.keys()))


# ---------------------------------------------------------------------------
# 19. Attachments
# ---------------------------------------------------------------------------

def demonstrate_attachments(client: ServiceNowClient, record_sys_id: str):
    """List and download attachments for a record."""
    gr = client.GlideRecord('incident')
    if gr.get(record_sys_id):
        attachments = gr.get_attachments()
        print(f"Attachments for {gr.number}: {len(attachments)}")
        for att in attachments:
            print(f"  {att.file_name} ({att.size_bytes} bytes)")
            link = att.get_link()
            print(f"  Download: {link}")


def upload_attachment(client: ServiceNowClient, record_sys_id: str):
    """Upload a file attachment to a record."""
    gr = client.GlideRecord('incident')
    if gr.get(record_sys_id):
        content = b"Hello from PySNC fluent example!"
        location = gr.add_attachment('example.txt', content, content_type='text/plain')
        print(f"Uploaded attachment: {location}")


# ---------------------------------------------------------------------------
# 20. Batch API
# ---------------------------------------------------------------------------

def demonstrate_batch_api(client: ServiceNowClient):
    """Use the Batch API to retrieve multiple records in a single HTTP call."""
    results = {}

    def make_hook(key):
        def hook(response):
            if response and response.status_code == 200:
                results[key] = response.json()['result']
        return hook

    # Queue multiple GET requests
    gr1 = client.GlideRecord('sys_user')
    gr1.fields = ['user_name', 'name']
    gr1.limit = 3
    client.batch_api.list(gr1, make_hook('users'))

    gr2 = client.GlideRecord('incident')
    gr2.fields = ['number', 'short_description']
    gr2.limit = 3
    client.batch_api.list(gr2, make_hook('incidents'))

    # Execute all queued requests in one HTTP call
    client.batch_api.execute()

    for key, records in results.items():
        print(f"\n{key} ({len(records)} records):")
        for rec in records:
            print(f"  {rec}")


# ---------------------------------------------------------------------------
# 21. Pop Record (Clone Current Record)
# ---------------------------------------------------------------------------

def demonstrate_pop_record(client: ServiceNowClient):
    """Clone the current record into a standalone GlideRecord."""
    gr = client.GlideRecord('incident')
    gr.add_active_query()
    gr.fields = ['number', 'short_description']
    gr.limit = 3
    gr.query()

    if gr.next():
        # Pop creates an independent copy of just the current record
        popped = gr.pop_record()
        print(f"Original position: {gr.location}, record: {gr.number}")
        print(f"Popped record: {popped.number} (independent copy)")

        # Original can keep iterating
        if gr.next():
            print(f"Next original: {gr.number}")


# ---------------------------------------------------------------------------
# 22. Rewindable vs Non-Rewindable Iteration
# ---------------------------------------------------------------------------

def demonstrate_rewindable(client: ServiceNowClient):
    """Show the difference between rewindable and non-rewindable records."""
    # Rewindable (default) - can iterate multiple times
    gr = client.GlideRecord('sys_user', rewindable=True)
    gr.add_active_query()
    gr.fields = ['user_name']
    gr.limit = 3
    gr.query()

    print("First pass:")
    for record in gr:
        print(f"  {record.user_name}")

    print("Second pass (rewind):")
    for record in gr:
        print(f"  {record.user_name}")

    # Non-rewindable - saves memory for large datasets
    gr2 = client.GlideRecord('sys_user', rewindable=False)
    gr2.add_active_query()
    gr2.fields = ['user_name']
    gr2.limit = 3
    gr2.query()

    print("\nNon-rewindable (single pass):")
    for record in gr2:
        print(f"  {record.user_name}")
    # Cannot iterate again - the records have been collected


# ---------------------------------------------------------------------------
# 23. Update Sets (ServiceNow-specific)
# ---------------------------------------------------------------------------

def list_update_sets(client: ServiceNowClient):
    """List update sets with their state."""
    gr = client.GlideRecord('sys_update_set')
    gr.add_query('state', 'complete')
    gr.fields = ['name', 'state', 'description', 'application', 'sys_created_on']
    gr.order_by_desc('sys_created_on')
    gr.limit = 10
    gr.query()

    print(f"Complete update sets: {gr.get_row_count()}")
    for record in gr:
        print(f"  {record.get_display_value('name')}")
        print(f"    State: {record.get_display_value('state')}")
        print(f"    App: {record.get_display_value('application')}")
        print(f"    Created: {record.sys_created_on}")
        print()


# ---------------------------------------------------------------------------
# 24. Script Includes (DevNow CICD App)
# ---------------------------------------------------------------------------

def list_script_includes(client: ServiceNowClient, scope: str = 'x_snc_devnow_cicd'):
    """List script includes for a given application scope."""
    gr = client.GlideRecord('sys_script_include')
    gr.add_query('sys_scope.scope', scope)
    gr.fields = ['name', 'api_name', 'description', 'active']
    gr.order_by('name')
    gr.query()

    print(f"Script includes for {scope}: {gr.get_row_count()}")
    for record in gr:
        status = "Active" if record.active == 'true' else "Inactive"
        print(f"  [{status}] {record.name}")
        if record.get_value('description'):
            desc = str(record.description)[:80]
            print(f"    {desc}")


# ---------------------------------------------------------------------------
# 25. Business Rules
# ---------------------------------------------------------------------------

def list_business_rules(client: ServiceNowClient, scope: str = 'x_snc_devnow_cicd'):
    """List business rules for a given application scope."""
    gr = client.GlideRecord('sys_script')
    gr.add_query('sys_scope.scope', scope)
    gr.fields = ['name', 'table', 'when', 'active', 'order']
    gr.add_active_query()
    gr.order_by('table')
    gr.query()

    print(f"Active business rules for {scope}: {gr.get_row_count()}")
    for record in gr:
        print(f"  {record.name} | Table: {record.table} | When: {record.when} | Order: {record.order}")


# ---------------------------------------------------------------------------
# 26. Client Scripts
# ---------------------------------------------------------------------------

def list_client_scripts(client: ServiceNowClient, scope: str = 'x_snc_devnow_cicd'):
    """List client scripts (note: table is sys_script_client, NOT sys_client_script)."""
    gr = client.GlideRecord('sys_script_client')
    gr.add_query('sys_scope.scope', scope)
    gr.fields = ['name', 'table', 'type', 'active', 'description']
    gr.order_by('name')
    gr.query()

    print(f"Client scripts for {scope}: {gr.get_row_count()}")
    for record in gr:
        status = "Active" if record.active == 'true' else "Inactive"
        print(f"  [{status}] {record.name} | Type: {record.type} | Table: {record.table}")


# ---------------------------------------------------------------------------
# 27. Async Example
# ---------------------------------------------------------------------------

ASYNC_EXAMPLE = '''
import asyncio
from pysnc.asyncio import AsyncServiceNowClient

async def async_query_example():
    """Demonstrate async GlideRecord usage."""
    client = AsyncServiceNowClient(
        'https://yourinstance.service-now.com',
        ('admin', 'password')
    )

    gr = await client.GlideRecord('incident')
    gr.add_active_query()
    gr.fields = ['number', 'short_description', 'priority']
    gr.limit = 5
    await gr.query()

    # Async iteration with 'async for'
    async for record in gr:
        print(f"{record.number} | {record.short_description}")

    # Async get
    gr2 = await client.GlideRecord('sys_user')
    if await gr2.get('user_name', 'admin'):
        print(f"Admin user: {gr2.get_display_value('name')}")

    # Async insert
    gr3 = await client.GlideRecord('incident')
    gr3.initialize()
    gr3.set_value('short_description', 'Async PySNC Example')
    gr3.set_value('priority', '3')
    sys_id = await gr3.insert()
    print(f"Created: {sys_id}")

    # Async serialize_all
    gr4 = await client.GlideRecord('incident')
    gr4.add_active_query()
    gr4.fields = ['number', 'short_description']
    gr4.limit = 3
    await gr4.query()
    all_data = await gr4.serialize_all(display_value=False)
    print(f"Serialized {len(all_data)} records")

    # Cleanup
    await client.close()

# asyncio.run(async_query_example())
'''


# ---------------------------------------------------------------------------
# 28. ITSM Stories
# ---------------------------------------------------------------------------

def list_stories(client: ServiceNowClient):
    """List recent stories from the rm_story table."""
    gr = client.GlideRecord('rm_story')
    gr.fields = ['number', 'short_description', 'state', 'acceptance_criteria']
    gr.order_by_desc('sys_created_on')
    gr.limit = 5
    gr.query()

    print(f"Recent stories: {gr.get_row_count()}")
    for record in gr:
        print(f"  {record.number} | {record.short_description}")
        print(f"    State: {record.get_display_value('state')}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("=" * 60)
    print("PySNC Fluent API Examples")
    print("=" * 60)

    client = get_client()

    examples = [
        ("1. Basic Query & Iteration", lambda: query_active_incidents(client)),
        ("2. Get by sys_id", lambda: get_user_by_sys_id(client, '6816f79cc0a8016401c5a33be04be441')),
        ("3. Get by field value", lambda: get_user_by_username(client, 'admin')),
        ("4. Complex query with operators", lambda: query_high_priority_incidents(client)),
        ("5. Encoded query", lambda: query_with_encoded_query(client)),
        ("6. Null/Not-Null queries", lambda: query_unassigned_incidents(client)),
        ("7. Join query", lambda: query_users_who_manage_groups(client)),
        ("8. RL query", lambda: query_users_with_admin_role(client)),
        ("9. Display values", lambda: demonstrate_display_values(client)),
        ("10. GlideElement access", lambda: demonstrate_glide_element(client)),
        ("17. Serialization", lambda: demonstrate_serialization(client)),
        ("18. Pandas export", lambda: demonstrate_pandas_export(client)),
        ("20. Batch API", lambda: demonstrate_batch_api(client)),
        ("21. Pop record", lambda: demonstrate_pop_record(client)),
        ("22. Rewindable iteration", lambda: demonstrate_rewindable(client)),
        ("23. Update sets", lambda: list_update_sets(client)),
        ("24. Script includes", lambda: list_script_includes(client)),
        ("25. Business rules", lambda: list_business_rules(client)),
        ("26. Client scripts", lambda: list_client_scripts(client)),
        ("28. Stories", lambda: list_stories(client)),
    ]

    # Run read-only examples by default; write examples need --write flag
    run_writes = '--write' in sys.argv

    for name, fn in examples:
        print(f"\n--- {name} ---")
        try:
            fn()
        except Exception as e:
            print(f"  Error: {e}")

    if run_writes:
        print("\n--- 11. Insert a Record ---")
        new_sys_id = insert_incident(client)

        if new_sys_id:
            print("\n--- 12. Update a Record ---")
            update_incident(client, new_sys_id)

            print("\n--- 13. Attribute-Style Update ---")
            update_with_attribute_style(client, new_sys_id)

            print("\n--- 14. Delete a Record ---")
            delete_incident(client, new_sys_id)

        print("\n--- 15. Batch Delete ---")
        delete_test_incidents(client)
    else:
        print("\n(Skipping write examples. Pass --write to run insert/update/delete demos.)")

    print("\n--- 27. Async Example (code only) ---")
    print(ASYNC_EXAMPLE)

    client.session.close()
    print("\nDone.")


if __name__ == '__main__':
    main()
