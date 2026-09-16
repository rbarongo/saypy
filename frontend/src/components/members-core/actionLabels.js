// Shared vocabulary for MemberAuditLog.action codes (see member_service.py's
// log_action() call sites for the exhaustive list) — used both by
// MemberHistoryModal's per-member history table and the Membership
// Movements report's action filter/labeling.
export const ACTION_LABELS = {
  created: 'Created',
  updated: 'Updated',
  registration_submitted: 'Registration submitted for review',
  registration_approved: 'Registration approved',
  registration_rejected: 'Registration rejected',
  transferred_out: 'Transferred out',
  correction_approved: 'Correction approved',
  correction_rejected: 'Correction rejected',
}

// Matches reports.py's DEFAULT_MOVEMENT_ACTIONS — the "movement" subset
// selected by default when the Membership Movements report first loads.
export const DEFAULT_MOVEMENT_ACTIONS = [
  'created', 'registration_submitted', 'registration_approved', 'registration_rejected', 'transferred_out',
]

export const ALL_ACTIONS = Object.keys(ACTION_LABELS)
