import React from 'react'
import { useEffect, useState } from 'react'
import Modal from './Modal'

export default function ColumnPicker({ open, title = 'Column Picker', allColumns, currentColumns, labelForColumn = (c) => c, onApply, onCancel }){
  const [left, setLeft] = useState([])
  const [right, setRight] = useState([])
  const [leftSelected, setLeftSelected] = useState(new Set())
  const [rightSelected, setRightSelected] = useState(new Set())

  useEffect(()=>{
    if(!open) return
    setLeft(allColumns.filter(c => !currentColumns.includes(c)))
    setRight(currentColumns)
    setLeftSelected(new Set())
    setRightSelected(new Set())
  }, [open])

  if(!open) return null

  function toggleLeft(col){
    const next = new Set(leftSelected)
    if(next.has(col)) next.delete(col); else next.add(col)
    setLeftSelected(next)
  }

  function toggleRight(col){
    const next = new Set(rightSelected)
    if(next.has(col)) next.delete(col); else next.add(col)
    setRightSelected(next)
  }

  function moveToRight(){
    const selected = Array.from(leftSelected)
    if(!selected.length) return
    setLeft(left.filter(c => !selected.includes(c)))
    setRight([...right, ...selected.filter(c => !right.includes(c))])
    setLeftSelected(new Set())
  }

  function moveToLeft(){
    const selected = Array.from(rightSelected)
    if(!selected.length) return
    setRight(right.filter(c => !selected.includes(c)))
    setLeft([...left, ...selected.filter(c => !left.includes(c))])
    setRightSelected(new Set())
  }

  function moveUp(){
    const selected = Array.from(rightSelected)
    if(selected.length !== 1) return
    const col = selected[0]
    const idx = right.findIndex(c => c === col)
    if(idx <= 0) return
    const next = [...right]
    ;[next[idx - 1], next[idx]] = [next[idx], next[idx - 1]]
    setRight(next)
  }

  function moveDown(){
    const selected = Array.from(rightSelected)
    if(selected.length !== 1) return
    const col = selected[0]
    const idx = right.findIndex(c => c === col)
    if(idx < 0 || idx >= right.length - 1) return
    const next = [...right]
    ;[next[idx + 1], next[idx]] = [next[idx], next[idx + 1]]
    setRight(next)
  }

  return (
    <Modal title={title} onClose={onCancel} maxWidth={800}>
      <div className='dual-list'>
        <div>
          <div className='dual-list-col-label'>AVAILABLE COLUMNS</div>
          <div className='dual-list-box'>
            {left.length === 0 ? (
              <div className='small' style={{padding:12,textAlign:'center'}}>No available columns</div>
            ) : left.map(col => (
              <div
                key={col}
                onClick={()=>toggleLeft(col)}
                className={`dual-list-item${leftSelected.has(col) ? ' dual-list-item--selected' : ''}`}
              >
                {labelForColumn(col)}
              </div>
            ))}
          </div>
        </div>

        <div>
          <div className='dual-list-col-label'>SELECTED COLUMNS</div>
          <div className='dual-list-box'>
            {right.length === 0 ? (
              <div className='small' style={{padding:12,textAlign:'center'}}>No selected columns</div>
            ) : right.map((col, idx) => (
              <div
                key={col}
                onClick={()=>toggleRight(col)}
                className={`dual-list-item${rightSelected.has(col) ? ' dual-list-item--selected' : ''}`}
                style={{display:'flex',justifyContent:'space-between',alignItems:'center'}}
              >
                <span>{labelForColumn(col)}</span>
                <span className='small'>{idx + 1}</span>
              </div>
            ))}
          </div>
          {rightSelected.size === 1 && (
            <div className='flex gap8 mt8'>
              <button type='button' className='btn btn-sm' style={{flex:1}} onClick={moveUp}>Move Up</button>
              <button type='button' className='btn btn-sm' style={{flex:1}} onClick={moveDown}>Move Down</button>
            </div>
          )}
        </div>
      </div>

      <div className='dual-list-actions' style={{flexDirection:'row',marginTop:14}}>
        <button type='button' className='btn btn-sm' disabled={leftSelected.size === 0} onClick={moveToRight}>Add &rarr;</button>
        <button type='button' className='btn btn-sm' disabled={rightSelected.size === 0} onClick={moveToLeft}>&larr; Remove</button>
      </div>

      <div className='flex gap8 mt8' style={{justifyContent:'flex-end'}}>
        <button type='button' className='btn' onClick={onCancel}>Cancel</button>
        <button type='button' className='btn btn-primary' onClick={()=>onApply(right)}>Apply</button>
      </div>
    </Modal>
  )
}
