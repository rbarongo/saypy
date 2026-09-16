import React from 'react'

export default function Badge({ level = 'info', children }){
  return <span className={`badge text-bg-${level}`}>{children}</span>
}
