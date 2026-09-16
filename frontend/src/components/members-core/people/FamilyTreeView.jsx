import React from 'react'
import { Star, Shield, Heart, Smile, Award, User } from 'react-feather'

const PARTNER_CODES = new Set(['head_of_family', 'spouse'])
const DEPENDENT_CODES = new Set(['child', 'dependent'])

function getInitials(firstName, lastName) {
  return `${firstName?.[0] || ''}${lastName?.[0] || ''}`.toUpperCase()
}

function getRoleMeta(code) {
  switch (code) {
    case 'head_of_family':
      return { label: 'Head of Family', color: 'var(--accent)', bg: 'var(--accent-soft)', icon: Shield }
    case 'spouse':
      return { label: 'Spouse', color: '#db2777', bg: '#fdf2f8', icon: Heart }
    case 'child':
      return { label: 'Child', color: '#0284c7', bg: '#f0f9ff', icon: Smile }
    case 'dependent':
      return { label: 'Dependent', color: '#7c3aed', bg: '#f5f3ff', icon: Award }
    default:
      return { label: 'Member', color: '#4b5563', bg: '#f3f4f6', icon: User }
  }
}

function FamilyTreeNode({ member, isLeader, onSetLeader }) {
  const meta = getRoleMeta(member.household_role.code)
  const IconComponent = meta.icon
  const initials = getInitials(member.first_name, member.last_name)

  return (
    <div className={`family-tree-node-card ${isLeader ? 'family-tree-node-card--leader' : ''}`}>
      {isLeader && (
        <div className="family-tree-node-leader-badge">
          <Star size={10} fill="currentColor" /> Leader
        </div>
      )}
      
      <div className="family-tree-node-avatar" style={{ backgroundColor: meta.color }}>
        {initials}
      </div>

      <div className="family-tree-node-info">
        <div className="family-tree-node-name">
          {member.first_name} {member.last_name}
        </div>
        <div className="family-tree-node-role-badge" style={{ color: meta.color, backgroundColor: meta.bg }}>
          {IconComponent && <IconComponent size={10} style={{ marginRight: '4px' }} />}
          {member.household_role.name}
        </div>
      </div>

      {!isLeader && onSetLeader && (
        <button 
          type='button' 
          className='family-tree-node-set-leader' 
          onClick={() => onSetLeader(member.id)}
        >
          Make Leader
        </button>
      )}
    </div>
  )
}

/**
 * Redesigned hierarchical tree rendering of a family roster.
 * Displays partners at the top, branching out dynamically to dependents and other members.
 */
export default function FamilyTreeView({ roster, onSetLeader }) {
  const members = roster.members || []
  if (members.length === 0) {
    return <p className='small text-muted text-center py-4'>No members in this family yet.</p>
  }

  const leaderId = roster.leader?.id
  const partners = members.filter(m => PARTNER_CODES.has(m.household_role.code))
  const dependents = members.filter(m => DEPENDENT_CODES.has(m.household_role.code))
  const placed = new Set([...partners, ...dependents].map(m => m.id))
  const others = members.filter(m => !placed.has(m.id))

  const hasSubTiers = dependents.length > 0 || others.length > 0
  const hasBothSubTiers = dependents.length > 0 && others.length > 0

  return (
    <div className='family-tree-wrapper'>
      <ul className='family-tree-root'>
        <li>
          <div className='parent-couple'>
            {partners.length > 0 ? (
              partners.map(m => (
                <FamilyTreeNode 
                  key={m.id} 
                  member={m} 
                  isLeader={m.id === leaderId} 
                  onSetLeader={onSetLeader} 
                />
              ))
            ) : (
              <div className='family-tree-virtual-root'>Family Roster</div>
            )}
          </div>

          {hasSubTiers && (
            <ul>
              {hasBothSubTiers ? (
                <>
                  <li>
                    <div className='tier-header-badge'>Dependents</div>
                    <ul>
                      {dependents.map(d => (
                        <li key={d.id}>
                          <FamilyTreeNode 
                            member={d} 
                            isLeader={d.id === leaderId} 
                            onSetLeader={onSetLeader} 
                          />
                        </li>
                      ))}
                    </ul>
                  </li>
                  <li>
                    <div className='tier-header-badge'>Other Members</div>
                    <ul>
                      {others.map(o => (
                        <li key={o.id}>
                          <FamilyTreeNode 
                            member={o} 
                            isLeader={o.id === leaderId} 
                            onSetLeader={onSetLeader} 
                          />
                        </li>
                      ))}
                    </ul>
                  </li>
                </>
              ) : dependents.length > 0 ? (
                dependents.map(d => (
                  <li key={d.id}>
                    <FamilyTreeNode 
                      member={d} 
                      isLeader={d.id === leaderId} 
                      onSetLeader={onSetLeader} 
                    />
                  </li>
                ))
              ) : (
                others.map(o => (
                  <li key={o.id}>
                    <FamilyTreeNode 
                      member={o} 
                      isLeader={o.id === leaderId} 
                      onSetLeader={onSetLeader} 
                    />
                  </li>
                ))
              )}
            </ul>
          )}
        </li>
      </ul>
    </div>
  )
}
