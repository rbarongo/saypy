import React, { useEffect, useState } from 'react'
import {
  Home, Sliders, Grid, BarChart2, Users, User, MapPin, Shield, Lock, Tag, Key,
  UserCheck, Settings, Phone, Award, Activity, Flag, Gift, Clipboard, ChevronDown,
  UploadCloud, CheckSquare, List, Clock, Cpu, Layers,
} from 'react-feather'
import useMyPermissions from '../hooks/useMyPermissions'

// The app now runs against a single backend: api/ (port 8001). It only implements
// auth + members/families so far, so these pages — still built against
// backend_old's routes/shapes — stay visible (so the eventual shape of the app is
// clear) but are disabled until they're rebuilt on the new backend.
const NOT_YET_ON_NEW_BACKEND = new Set(['collections', 'members', 'members_collections', 'reports', 'admin', 'settings'])
const DISABLED_TITLE = 'Not yet available on the new backend (api/)'

const NAVIGATION_TREE = [
  // Past (Legacy) & Current root pages
  { key: 'dashboard', label: 'Dashboard', icon: Home, route: { menu: 'dashboard' } },
  // { key: 'preferences', label: 'Preferences', icon: Sliders, page: 'preferences' },
  // { key: 'collections', label: 'Collections', icon: Gift, page: 'collections' },
  // { key: 'members_legacy', label: 'Members (Legacy)', icon: Users, page: 'members' },
  // { key: 'members_collections', label: 'Members Collections', icon: Clipboard, page: 'members_collections' },
  // { key: 'reports', label: 'Reports', icon: BarChart2, page: 'reports' },
  // { key: 'admin', label: 'Admin', icon: Shield, page: 'admin' },
  // { key: 'settings', label: 'Settings', icon: Settings, page: 'settings' },
  { key: 'stp_configuration', label: 'Configuration', icon: Settings, route: { menu: 'configuration' } },
  { key: 'stp_church_management', label: 'Church Management', icon: MapPin, route: { menu: 'church_management' } },
  {
    key: 'stp_user_management', label: 'User Management', icon: Shield,
    children: [
      { key: 'stp_access_control', label: 'Access Control', icon: Lock, route: { menu: 'user_management', sub: 'access_control' } },
      { key: 'stp_accounts', label: 'User Accounts', icon: UserCheck, route: { menu: 'user_management', sub: 'accounts' } },
    ],
  },
  {
    key: 'stp_member_management', label: 'Member Management', icon: Users,
    children: [
      { key: 'stp_members', label: 'Members', icon: User, route: { menu: 'member_management', sub: 'members' } },
      { key: 'stp_families', label: 'Families', icon: Users, route: { menu: 'member_management', sub: 'families' } },
      { key: 'stp_pending_registrations', label: 'Pending Registrations', icon: Clock, route: { menu: 'member_management', sub: 'pending_registrations' } },
      { key: 'stp_correction_requests', label: 'Correction Requests', icon: CheckSquare, route: { menu: 'member_management', sub: 'correction_requests' } },
      { key: 'stp_reports', label: 'Reports', icon: BarChart2, route: { menu: 'member_management', sub: 'reports' } },
    ],
  },
  {
    key: 'stp_collection_management', label: 'Collections', icon: Gift,
    children: [
      { key: 'stp_collection_types', label: 'Collection Types', icon: Tag, route: { menu: 'collection_management', sub: 'types' }, anyOf: ['manage_collection_types', 'manage_collections', 'review_collections', 'view_collections'] },
      { key: 'stp_collections_list', label: 'Collections', icon: List, route: { menu: 'collection_management', sub: 'collections' }, anyOf: ['manage_collections', 'review_collections', 'view_collections'] },
      { key: 'stp_collections_import', label: 'Import', icon: UploadCloud, route: { menu: 'collection_management', sub: 'import' }, anyOf: ['manage_collections'] },
      { key: 'stp_collections_groups', label: 'Review Groups', icon: CheckSquare, route: { menu: 'collection_management', sub: 'groups' }, anyOf: ['manage_collections', 'review_collections', 'view_collections'] },
      { key: 'stp_agent_submissions', label: 'Digital Receipts', icon: Cpu, route: { menu: 'collection_management', sub: 'agent_submissions' }, anyOf: ['manage_collections', 'review_collections', 'view_collections'] },
      { key: 'stp_receipt_series', label: 'Receipt Series', icon: Key, route: { menu: 'collection_management', sub: 'receipt_series' }, anyOf: ['manage_receipts'] },
      { key: 'stp_collection_audit_log', label: 'Audit Log', icon: Clock, route: { menu: 'collection_management', sub: 'audit_log' }, anyOf: ['view_treasury_audit'] },
    ],
  },
]

// Applied only to the Collections/Treasury subtree for now (see A1) — a
// node with `anyOf` is kept only if the caller has at least one of those
// permission codes; a parent survives if any child does. Everything else in
// the tree has no `anyOf` and passes through unfiltered, unchanged.
function filterTreeByPermission(nodes, can) {
  return nodes.reduce((acc, node) => {
    if (node.children) {
      const children = filterTreeByPermission(node.children, can)
      if (children.length > 0) acc.push({ ...node, children })
      return acc
    }
    if (node.anyOf && !node.anyOf.some(code => can(code))) return acc
    acc.push(node)
    return acc
  }, [])
}

function routeMatches(a, b) {
  if (!a || !b) return false
  return a.menu === b.menu && (a.sub || null) === (b.sub || null) && (a.subsub || null) === (b.subsub || null)
}

function findActivePathKeys(nodes, page, setupRoute, trail = []) {
  for (const node of nodes) {
    const isActive = node.page
      ? page === node.page
      : (node.route && page === 'members_core' && routeMatches(node.route, setupRoute))

    if (isActive) return trail

    if (node.children) {
      const result = findActivePathKeys(node.children, page, setupRoute, [...trail, node.key])
      if (result) return result
    }
  }
  return null
}

function NavigationTreeNode({ node, depth, expanded, onToggle, activePage, activeRoute, onSelectPage, onSelectRoute }) {
  const hasChildren = !!node.children
  const isExpanded = expanded.has(node.key)
  const isActive = node.page
    ? activePage === node.page
    : (node.route && activePage === 'members_core' && routeMatches(node.route, activeRoute))
  const Icon = node.icon
  const isDisabled = !!(node.page && NOT_YET_ON_NEW_BACKEND.has(node.page))

  function handleClick(e) {
    e.preventDefault()
    if (isDisabled) return
    if (hasChildren) {
      onToggle(node.key)
    } else if (node.page) {
      onSelectPage(node.page)
    } else if (node.route) {
      onSelectRoute(node.route)
    }
  }

  return (
    <li className={`sidebar-item${isActive ? ' active' : ''}${isDisabled ? ' disabled' : ''}`} style={{ paddingLeft: depth * 14 }}>
      <a
        className='sidebar-link'
        href='#'
        title={isDisabled ? DISABLED_TITLE : undefined}
        onClick={handleClick}
        style={{ display: 'flex', alignItems: 'center' }}
      >
        {Icon && <Icon className='align-middle sidebar-icon' size={17} />}
        <span className='align-middle'>{node.label}</span>
        {hasChildren && <ChevronDown className={`align-middle sidebar-tree-chevron${isExpanded ? ' sidebar-tree-chevron--open' : ''}`} size={15} />}
      </a>
      {hasChildren && isExpanded && (
        <ul className='sidebar-dropdown list-unstyled'>
          {node.children.map(child => (
            <NavigationTreeNode
              key={child.key}
              node={child}
              depth={depth + 1}
              expanded={expanded}
              onToggle={onToggle}
              activePage={activePage}
              activeRoute={activeRoute}
              onSelectPage={onSelectPage}
              onSelectRoute={onSelectRoute}
            />
          ))}
        </ul>
      )}
    </li>
  )
}

export default function SidebarNav({ page, setPage, setupRoute, setSetupRoute, appName, fetchUsers, open, onNavigate, token }) {
  const [expanded, setExpanded] = useState(() => new Set())
  const { can } = useMyPermissions(token)
  const visibleTree = filterTreeByPermission(NAVIGATION_TREE, can)

  useEffect(() => {
    const ancestors = findActivePathKeys(visibleTree, page, setupRoute)
    if (ancestors && ancestors.length) {
      setExpanded(prev => {
        const next = new Set(prev)
        ancestors.forEach(k => next.add(k))
        return next
      })
    }
  }, [page, setupRoute])

  function toggle(key) {
    setExpanded(prev => {
      const next = new Set(prev)
      if (next.has(key)) next.delete(key); else next.add(key)
      return next
    })
  }

  function handleSelectRoute(route) {
    setPage('members_core')
    setSetupRoute(route)
    if (onNavigate) onNavigate()
  }

  function handleSelectPage(nextPage) {
    setPage(nextPage)
    if (nextPage === 'admin' && fetchUsers) {
      fetchUsers()
    }
    if (onNavigate) onNavigate()
  }

  return (
    <>
      {open && <div className='sidebar-backdrop' onClick={onNavigate} />}
      <nav className={`sidebar js-sidebar${open ? ' collapsed' : ''}`}>
        <div className='sidebar-content'>
          <a className='sidebar-brand' href='#' onClick={e => e.preventDefault()}>
            <span className='sidebar-brand-mark'><Layers size={19} /></span>
            <span className='sidebar-brand-copy'>
              <span className='sidebar-brand-eyebrow'>Church</span>
              <span className='sidebar-brand-name'>{appName}</span>
            </span>
          </a>

          <ul className='sidebar-nav'>
            <li className='sidebar-header'>Navigation</li>
            {visibleTree.map(node => (
              <NavigationTreeNode
                key={node.key}
                node={node}
                depth={0}
                expanded={expanded}
                onToggle={toggle}
                activePage={page}
                activeRoute={setupRoute}
                onSelectPage={handleSelectPage}
                onSelectRoute={handleSelectRoute}
              />
            ))}
          </ul>
        </div>
      </nav>
    </>
  )
}
