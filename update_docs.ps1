$docPath = 'Documentation.docx'

# Load COM object
Add-Type -AssemblyName 'System.Runtime.InteropServices'
$word = New-Object -ComObject Word.Application
$word.Visible = $false

try {
    # Open docx
    $full_path = (Get-Item $docPath).FullName
    $doc = $word.Documents.Open($full_path, $false, $false, $true)

    # Go to end
    $range = $doc.Content
    $range.Collapse(2)

    # Add new section with spacing
    $range.InsertAfter([char]13 + [char]13)
    
    # Add heading
    $heading = $doc.Range()
    $heading.InsertAfter('LOCAL ADMIN: VIEWING COLLECTION CODES' + [char]13)
    $heading.Collapse(2)
    
    # Add body text
    $body = @'
The new `can_view_collection_codes` permission allows local admins and other roles to view existing collection codes without necessarily having full management permissions.

Permission: can_view_collection_codes
- Applies to: system_admin, admin, treasurer, data_steward, uploader, viewer
- Effect: Users with this right can view all existing collection codes in their church on the Collections page
- Behavior: 
  - Collection codes will be visible in a read-only table
  - Only users with `can_manage_collections` can see the "Add New Code" section
  - Only users with `can_manage_collections` can edit or delete codes

Frontend Behavior:
- Collections menu button shows for users with either `can_manage_collections` OR `can_view_collection_codes`
- Collections page displays existing codes to viewers
- Management controls (Add, Edit, Delete) only appear for admins with `can_manage_collections` right

Database Schema:
- Table: role_policies
- New column: can_view_collection_codes (INTEGER, DEFAULT 0)
- Migration: ensure_role_policies_schema() adds column if missing and backfills for built-in roles

Operational Checklist:
✓ Backend restart applies migration to add can_view_collection_codes column
✓ Built-in roles (admin, treasurer, viewer, etc.) auto-populated with can_view_collection_codes=1
✓ Frontend renders Collection codes view for users with can_view_collection_codes right
✓ Edit/Delete buttons only appear for users with can_manage_collections right
'@

    $body_range = $doc.Range()
    $body_range.InsertAfter($body)
    $body_range.Font.Name = 'Calibri'
    $body_range.Font.Size = 11

    # Save
    $doc.Save()
    $doc.Close()
    Write-Host 'Documentation updated successfully'
} finally {
    $word.Quit()
}
