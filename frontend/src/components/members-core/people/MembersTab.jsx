import React, { useEffect, useState } from 'react'
import { Clock, Edit2, LogOut, Trash2, User } from 'react-feather'
import Alert from '../../Alert'
import Badge from '../../Badge'
import PageHeader from '../PageHeader'
import StatCard, { StatGrid } from '../StatCard'
import DataTable from '../DataTable'
import { membersApi } from '../api'
import { fullName, statusBadgeLevel } from '../constants'
import useReferenceData from '../useReferenceData'
import MemberForm from './MemberForm'
import MemberHistoryModal from './MemberHistoryModal'
import TransferMemberModal from './TransferMemberModal'

const PAGE_SIZE = 10
const EMPTY_FILTERS = { church_id: '', household_role_id: '', membership_category_id: '', status_id: '' }
const STATUS_COLOR = { active: 'success', inactive: 'warning', transferred: 'info', deceased: 'danger' }

export default function MembersTab(){
  const ref = useReferenceData()

  const [members, setMembers] = useState([])
  const [total, setTotal] = useState(0)
  const [page, setPage] = useState(1)
  const [search, setSearch] = useState('')
  const [sort, setSort] = useState({ key: 'last_name', dir: 'asc' })
  const [filters, setFilters] = useState(EMPTY_FILTERS)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')

  const [showForm, setShowForm] = useState(false)
  const [editingMember, setEditingMember] = useState(null)
  const [historyFor, setHistoryFor] = useState(null)
  const [transferFor, setTransferFor] = useState(null)

  const [stats, setStats] = useState(null)

  const churchById = new Map(ref.churches.map(c => [c.id, c]))

  async function refreshMembers(){
    setLoading(true); setError('')
    try{
      const sortParam = sort.key ? (sort.dir === 'desc' ? `-${sort.key}` : sort.key) : undefined
      const res = await membersApi.listMembers({ page, page_size: PAGE_SIZE, q: search, sort: sortParam, ...filters })
      setMembers(res.items || [])
      setTotal(res.total || 0)
    }catch(e){
      setError(e.message || 'Failed to load members')
    }finally{
      setLoading(false)
    }
  }

  useEffect(() => { refreshMembers() }, [page, search, sort.key, sort.dir, filters.church_id, filters.household_role_id, filters.membership_category_id, filters.status_id]) // eslint-disable-line react-hooks/exhaustive-deps
  useEffect(() => { membersApi.getStatsOverview().then(setStats).catch(() => {}) }, [])

  function setFilter(key, value){ setFilters(prev => ({ ...prev, [key]: value })); setPage(1) }
  function handleSearch(value){ setSearch(value); setPage(1) }
  function handleSort(key, dir){ setSort({ key, dir }); setPage(1) }

  function openNew(){ setEditingMember(null); setShowForm(true) }
  function openEdit(m){ setEditingMember(m); setShowForm(true) }
  function closeForm(){ setShowForm(false); setEditingMember(null) }

  async function handleSaved(id, opts){
    await refreshMembers()
    membersApi.getStatsOverview().then(setStats).catch(() => {})
    if(opts && opts.keepOpen && id){
      try{ setEditingMember(await membersApi.getMember(id)) }catch(e){ /* keep previous */ }
    }else{
      closeForm()
    }
  }

  async function handleDelete(m){
    if(!window.confirm(`Delete ${fullName(m)}?`)) return
    try{
      await membersApi.deleteMember(m.id)
      await refreshMembers()
    }catch(e){
      setError(e.message || 'Delete failed')
    }
  }

  if(ref.loading) return <p className='small'>Loading reference data…</p>
  if(ref.error) return <Alert level='error' title='Failed to load reference data'>{ref.error}</Alert>

  const columns = [
    { key: 'member_number', label: 'Member #', sortable: false },
    { key: 'first_name', label: 'Name', render: fullName, sortValue: fullName },
    { key: 'church', label: 'Church', sortable: false, render: m => churchById.get(m.church_id)?.name || m.church_id },
    { key: 'household_role', label: 'Household Role', sortable: false, render: m => m.household_role?.name },
    { key: 'membership_category', label: 'Membership', sortable: false, render: m => m.membership_category?.name },
    { key: 'status', label: 'Status', sortable: false, render: m => <Badge level={statusBadgeLevel(m.status?.code)}>{m.status?.name}</Badge> },
    { key: 'family_name', label: 'Family', sortable: false, render: m => m.family_name || '—' },
    {
      key: 'contact', label: 'Primary Contact', sortable: false,
      render: m => { const p = (m.contacts || []).find(c => c.contact_init) || (m.contacts || [])[0]; return p ? p.contact : '—' },
    },
  ]

  return (
    <div>
      <PageHeader
        title='Member Management'
        subtitle='Members'
        icon={User}
        actions={<button type='button' className='btn btn-primary' onClick={openNew}>+ New Member</button>}
      />

      {stats && (
        <StatGrid>
          <StatCard label='Total Members' value={stats.members_total} icon={User} color='primary' />
          {stats.members_by_status.map(s => (
            <StatCard key={s.code} label={s.name} value={s.count} color={STATUS_COLOR[s.code] || 'info'} />
          ))}
        </StatGrid>
      )}

      <div className='toolbar'>
        <select className='form-select' style={{ maxWidth: 220 }} value={filters.church_id} onChange={e => setFilter('church_id', e.target.value)}>
          <option value=''>-- all churches --</option>
          {ref.churches.map(c => <option key={c.id} value={c.id}>{c.name}</option>)}
        </select>
        <select className='form-select' style={{ maxWidth: 220 }} value={filters.household_role_id} onChange={e => setFilter('household_role_id', e.target.value)}>
          <option value=''>-- household role --</option>
          {ref.householdRoles.map(r => <option key={r.id} value={r.id}>{r.name}</option>)}
        </select>
        <select className='form-select' style={{ maxWidth: 220 }} value={filters.membership_category_id} onChange={e => setFilter('membership_category_id', e.target.value)}>
          <option value=''>-- membership category --</option>
          {ref.membershipCategories.map(r => <option key={r.id} value={r.id}>{r.name}</option>)}
        </select>
        <select className='form-select' style={{ maxWidth: 220 }} value={filters.status_id} onChange={e => setFilter('status_id', e.target.value)}>
          <option value=''>-- status --</option>
          {ref.memberStatuses.map(r => <option key={r.id} value={r.id}>{r.name}</option>)}
        </select>
      </div>

      {error && <div className='mb8'><Alert level='error' title='Something went wrong'>{error}</Alert></div>}

      <DataTable
        columns={columns}
        rows={members}
        loading={loading}
        emptyMessage='No members found.'
        serverMode
        total={total}
        page={page}
        onPageChange={setPage}
        pageSize={PAGE_SIZE}
        sortKey={sort.key}
        sortDir={sort.dir}
        onSortChange={handleSort}
        searchValue={search}
        onSearchChange={handleSearch}
        actions={m => (
          <div className='flex gap8'>
            <button type='button' className='btn btn-sm btn-light' onClick={() => openEdit(m)} title='Edit'><Edit2 size={16} /></button>
            <button type='button' className='btn btn-sm btn-light' onClick={() => setHistoryFor(m)} title='History'><Clock size={16} /></button>
            <button type='button' className='btn btn-sm btn-light' onClick={() => setTransferFor(m)} title='Transfer out'><LogOut size={16} /></button>
            <button type='button' className='btn btn-sm btn-outline-danger' onClick={() => handleDelete(m)} title='Delete'><Trash2 size={16} /></button>
          </div>
        )}
      />

      <MemberForm
        open={showForm}
        onClose={closeForm}
        editingMember={editingMember}
        churches={ref.churches}
        referenceData={ref}
        defaultChurchId={filters.church_id}
        onSaved={handleSaved}
      />

      {historyFor && <MemberHistoryModal member={historyFor} onClose={() => setHistoryFor(null)} />}

      {transferFor && (
        <TransferMemberModal
          member={transferFor}
          churches={ref.churches}
          onClose={() => setTransferFor(null)}
          onTransferred={() => { setTransferFor(null); refreshMembers() }}
        />
      )}
    </div>
  )
}
