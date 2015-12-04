#include <assert.h>
#include "btree.h"
#include <stack>
KeyValuePair::KeyValuePair()
{}


KeyValuePair::KeyValuePair(const KEY_T &k, const VALUE_T &v) : 
  key(k), value(v)
{}


KeyValuePair::KeyValuePair(const KeyValuePair &rhs) :
  key(rhs.key), value(rhs.value)
{}


KeyValuePair::~KeyValuePair()
{}


KeyValuePair & KeyValuePair::operator=(const KeyValuePair &rhs)
{
  return *( new (this) KeyValuePair(rhs));
}

BTreeIndex::BTreeIndex(SIZE_T keysize, 
		       SIZE_T valuesize,
		       BufferCache *cache,
		       bool unique) 
{
  superblock.info.keysize=keysize;
  superblock.info.valuesize=valuesize;
  buffercache=cache;
  // note: ignoring unique now
}

BTreeIndex::BTreeIndex()
{
  // shouldn't have to do anything
}


//
// Note, will not attach!
//
BTreeIndex::BTreeIndex(const BTreeIndex &rhs)
{
  buffercache=rhs.buffercache;
  superblock_index=rhs.superblock_index;
  superblock=rhs.superblock;
}

BTreeIndex::~BTreeIndex()
{
  // shouldn't have to do anything
}


BTreeIndex & BTreeIndex::operator=(const BTreeIndex &rhs)
{
  return *(new(this)BTreeIndex(rhs));
}


ERROR_T BTreeIndex::AllocateNode(SIZE_T &n)
{
  n=superblock.info.freelist;

  if (n==0) { 
    return ERROR_NOSPACE;
  }

  BTreeNode node;

  node.Unserialize(buffercache,n);

  assert(node.info.nodetype==BTREE_UNALLOCATED_BLOCK);

  superblock.info.freelist=node.info.freelist;

  superblock.Serialize(buffercache,superblock_index);

  buffercache->NotifyAllocateBlock(n);

  return ERROR_NOERROR;
}


ERROR_T BTreeIndex::DeallocateNode(const SIZE_T &n)
{
  BTreeNode node;

  node.Unserialize(buffercache,n);

  assert(node.info.nodetype!=BTREE_UNALLOCATED_BLOCK);

  node.info.nodetype=BTREE_UNALLOCATED_BLOCK;

  node.info.freelist=superblock.info.freelist;

  node.Serialize(buffercache,n);

  superblock.info.freelist=n;

  superblock.Serialize(buffercache,superblock_index);

  buffercache->NotifyDeallocateBlock(n);

  return ERROR_NOERROR;

}

ERROR_T BTreeIndex::Attach(const SIZE_T initblock, const bool create)
{
  ERROR_T rc;

  superblock_index=initblock;
  assert(superblock_index==0);

  if (create) {
    // build a super block, root node, and a free space list
    //
    // Superblock at superblock_index
    // root node at superblock_index+1
    // free space list for rest
    BTreeNode newsuperblock(BTREE_SUPERBLOCK,
			    superblock.info.keysize,
			    superblock.info.valuesize,
			    buffercache->GetBlockSize());
    newsuperblock.info.rootnode=superblock_index+1;
    newsuperblock.info.freelist=superblock_index+2;
    newsuperblock.info.numkeys=0;

    buffercache->NotifyAllocateBlock(superblock_index);

    rc=newsuperblock.Serialize(buffercache,superblock_index);

    if (rc) { 
      return rc;
    }
    
    BTreeNode newrootnode(BTREE_ROOT_NODE,
			  superblock.info.keysize,
			  superblock.info.valuesize,
			  buffercache->GetBlockSize());
    newrootnode.info.rootnode=superblock_index+1;
    newrootnode.info.freelist=superblock_index+2;
    newrootnode.info.numkeys=0;

    buffercache->NotifyAllocateBlock(superblock_index+1);

    rc=newrootnode.Serialize(buffercache,superblock_index+1);

    if (rc) { 
      return rc;
    }

    for (SIZE_T i=superblock_index+2; i<buffercache->GetNumBlocks();i++) { 
      BTreeNode newfreenode(BTREE_UNALLOCATED_BLOCK,
			    superblock.info.keysize,
			    superblock.info.valuesize,
			    buffercache->GetBlockSize());
      newfreenode.info.rootnode=superblock_index+1;
      newfreenode.info.freelist= ((i+1)==buffercache->GetNumBlocks()) ? 0: i+1;
      
      rc = newfreenode.Serialize(buffercache,i);

      if (rc) {
	return rc;
      }

    }
  }

  // OK, now, mounting the btree is simply a matter of reading the superblock 

  return superblock.Unserialize(buffercache,initblock);
}
    

ERROR_T BTreeIndex::Detach(SIZE_T &initblock)
{
  return superblock.Serialize(buffercache,superblock_index);
}
 

ERROR_T BTreeIndex::LookupOrUpdateInternal(const SIZE_T &node,
					   const BTreeOp op,
					   const KEY_T &key,
					   VALUE_T &value)
{
  BTreeNode b;
  ERROR_T rc;
  SIZE_T offset;
  KEY_T testkey;
  SIZE_T ptr;

  rc= b.Unserialize(buffercache,node);

  if (rc!=ERROR_NOERROR) { 
    return rc;
  }

  switch (b.info.nodetype) { 
  case BTREE_ROOT_NODE:
  case BTREE_INTERIOR_NODE:
    // Scan through key/ptr pairs
    //and recurse if possible
    for (offset=0;offset<b.info.numkeys;offset++) { 
      rc=b.GetKey(offset,testkey);
      if (rc) {  return rc; }
      if (key<testkey || key==testkey) {
	// OK, so we now have the first key that's larger
	// so we ned to recurse on the ptr immediately previous to 
	// this one, if it exists
	rc=b.GetPtr(offset,ptr);
	if (rc) { return rc; }
	return LookupOrUpdateInternal(ptr,op,key,value);
      }
    }
    // if we got here, we need to go to the next pointer, if it exists
    if (b.info.numkeys>0) { 
      rc=b.GetPtr(b.info.numkeys,ptr);
      if (rc) { return rc; }
      return LookupOrUpdateInternal(ptr,op,key,value);
    } else {
      // There are no keys at all on this node, so nowhere to go
      return ERROR_NONEXISTENT;
    }
    break;
  case BTREE_LEAF_NODE:
    // Scan through keys looking for matching value
    for (offset=0;offset<b.info.numkeys;offset++) { 
      rc=b.GetKey(offset,testkey);
      if (rc) {  return rc; }
      if (testkey==key) { 
	if (op==BTREE_OP_LOOKUP) { 
	  return b.GetVal(offset,value);
	} else { 
	  // BTREE_OP_UPDATE
	  
	  // WRITE ME
	  //setval
	  rc = b.SetVal(offset,value);

      if(rc!=ERROR_NOERROR){
        return rc;
      } else {
        return b.Serialize(buffercache,node);
      }
	  //serialize
	  //return ERROR_UNIMPL;
	}
      }
    }
    return ERROR_NONEXISTENT;
    break;
  default:
    // We can't be looking at anything other than a root, internal, or leaf
    return ERROR_INSANE;
    break;
  }  

  return ERROR_INSANE;
}


static ERROR_T PrintNode(ostream &os, SIZE_T nodenum, BTreeNode &b, BTreeDisplayType dt)
{
  KEY_T key;
  VALUE_T value;
  SIZE_T ptr;
  SIZE_T offset;
  ERROR_T rc;
  unsigned i;

  if (dt==BTREE_DEPTH_DOT) { 
    os << nodenum << " [ label=\""<<nodenum<<": ";
  } else if (dt==BTREE_DEPTH) {
    os << nodenum << ": ";
  } else {
  }

  switch (b.info.nodetype) { 
  case BTREE_ROOT_NODE:
  case BTREE_INTERIOR_NODE:
    if (dt==BTREE_SORTED_KEYVAL) {
    } else {
      if (dt==BTREE_DEPTH_DOT) { 
      } else { 
	os << "Interior: ";
      }
      for (offset=0;offset<=b.info.numkeys;offset++) { 
	rc=b.GetPtr(offset,ptr);
	if (rc) { return rc; }
	os << "*" << ptr << " ";
	// Last pointer
	if (offset==b.info.numkeys) break;
	rc=b.GetKey(offset,key);
	if (rc) {  return rc; }
	for (i=0;i<b.info.keysize;i++) { 
	  os << key.data[i];
	}
	os << " ";
      }
    }
    break;
  case BTREE_LEAF_NODE:
    if (dt==BTREE_DEPTH_DOT || dt==BTREE_SORTED_KEYVAL) { 
    } else {
      os << "Leaf: ";
    }
    for (offset=0;offset<b.info.numkeys;offset++) { 
      if (offset==0) { 
	// special case for first pointer
	rc=b.GetPtr(offset,ptr);
	if (rc) { return rc; }
	if (dt!=BTREE_SORTED_KEYVAL) { 
	  os << "*" << ptr << " ";
	}
      }
      if (dt==BTREE_SORTED_KEYVAL) { 
	os << "(";
      }
      rc=b.GetKey(offset,key);
      if (rc) {  return rc; }
      for (i=0;i<b.info.keysize;i++) { 
	os << key.data[i];
      }
      if (dt==BTREE_SORTED_KEYVAL) { 
	os << ",";
      } else {
	os << " ";
      }
      rc=b.GetVal(offset,value);
      if (rc) {  return rc; }
      for (i=0;i<b.info.valuesize;i++) { 
	os << value.data[i];
      }
      if (dt==BTREE_SORTED_KEYVAL) { 
	os << ")\n";
      } else {
	os << " ";
      }
    }
    break;
  default:
    if (dt==BTREE_DEPTH_DOT) { 
      os << "Unknown("<<b.info.nodetype<<")";
    } else {
      os << "Unsupported Node Type " << b.info.nodetype ;
    }
  }
  if (dt==BTREE_DEPTH_DOT) { 
    os << "\" ]";
  }
  return ERROR_NOERROR;
}
  
ERROR_T BTreeIndex::Lookup(const KEY_T &key, VALUE_T &value)
{
  return LookupOrUpdateInternal(superblock.info.rootnode, BTREE_OP_LOOKUP, key, value);
}

ERROR_T BTreeIndex::Insert(const KEY_T &key, const VALUE_T &value)
{
  BTreeNode root;
  ERROR_T rc;
  SIZE_T offset;
  SIZE_T reverseoffset;
  KEY_T testkey;
  SIZE_T ptr;
  KeyValuePair kvpair = KeyValuePair(key,value);

  //Start with rootnode and check if it is empty (aka first insert)
  root.Unserialize(buffercache, superblock.info.rootnode);  

  if(root.info.numkeys == 0) {
    BTreeNode child(BTREE_LEAF_NODE, superblock.info.keysize, superblock.info.valuesize, buffercache->GetBlockSize());

    SIZE_T rootleft;
    SIZE_T rootright;
    
    rc = AllocateNode(rootleft);
    if(rc != ERROR_NOERROR) {return rc;}
    rc = AllocateNode(rootright);
    if(rc != ERROR_NOERROR) {return rc;}
    
    child.Serialize(buffercache, rootright);

    //im pretty sure the first key goes into the left?
    child.info.numkeys += 1;
    child.SetKeyVal(0, kvpair);
    child.Serialize(buffercache, rootleft);
    root.info.numkeys += 1;

    //the key that is inserted is that the keys less than it go to the left
    root.SetKey(0, key);
    root.SetPtr(0, rootleft);
    root.SetPtr(1, rootright);
    root.Serialize(buffercache, superblock.info.rootnode);

    return ERROR_NOERROR;
  }


  //initialize a stack that holds nodes
  std::stack<BTreeNode> traversednodes;
  //cout << "Traversing the tree\n\n-----------------\n";

  //traverse the tree, unserilizing nodes and inserting them, until we reach a leaf node
  while(root.info.nodetype != BTREE_LEAF_NODE){
    //cout << root <<"\n";
    traversednodes.push(root);
    //get the next node to go to, which is the pointer before the first key that is larger
    //than the key we have
    for(offset = 0; offset < root.info.numkeys; offset++) {
      rc = root.GetKey(offset,testkey);
      if(rc != ERROR_NOERROR) {return rc;}
      

      //did we find the fisrt key larger?
      if(key<testkey || key==testkey){
        break;
      }
    }
    
    rc = root.GetPtr(offset,ptr);
    if (rc != ERROR_NOERROR) {return rc; }
    
    root.Unserialize(buffercache,ptr);
  }
  

  //root is currently the laef node
  //cout << root.info.numkeys << " of " << root.info.GetNumSlotsAsLeaf() << " used." << "\n\n";
  //cout << root.info.GetNumSlotsAsInterior();

  //test printing the stack to make sure each node is difersnt
  //while(!traversednodes.empty())
 // {
  //  cout << traversednodes.top();
  //  cout << "\n\n";
  //  traversednodes.pop();
  //} 
  //upon reaching the leaf node, attempt to insert the key, this might be hard

  //get the offset of where to put the key
  
  if(root.info.numkeys >= root.info.GetNumSlotsAsLeaf()){
    //the leaf is full so we have to split it
    //allocate a new node
    KEY_T promote;
    SIZE_T newleafptr;
    BTreeNode newleaf;
    SIZE_T counter,othercounter;
    othercounter = 0;
    rc = AllocateNode(newleafptr);
    if(rc != ERROR_NOERROR) {return rc;}

    newleaf.Unserialize(buffercache,newleafptr); 
    //copy half of the keys from
    //split the keys into left and right keys
    SIZE_T leftsplit = root.info.numkeys / 2;
    SIZE_T rightsplit = root.info.numkeys - leftsplit;
    
    newleaf.info.numkeys = rightsplit;

    for (counter = leftsplit; counter < root.info.numkeys; counter++){
      rc = root.GetKeyVal(counter,kvpair);
      if (rc != ERROR_NOERROR) {return rc;}
      rc = newleaf.SetKeyVal(othercounter,kvpair);
      if (rc != ERROR_NOERROR) {return rc;}
    }
    
    root.info.numkeys = leftsplit;
    //between the two nodes, find out where to put the key val pair
    
    //todo
    root.GetKey(root.info.numkeys-1,testkey);

    if(key < testkey){
      //insert in root
      root.info.numkeys += 1;
      for (offset = 0; offset < root.info.numkeys; offset++){
        root.GetKey(offset,testkey);
        if(testkey < key) {
          break;
        }
      }

      for (reverseoffset = root.info.numkeys-1; reverseoffset >= offset && reverseoffset > 0; reverseoffset--){
        //shift keyvalue pairs over til we got the slot where we will place the key
        rc = root.GetKeyVal(reverseoffset-1,kvpair);
        if (rc != ERROR_NOERROR) {return rc;}
        rc = root.SetKeyVal(reverseoffset,kvpair);
        if (rc != ERROR_NOERROR) {return rc;}
      }
      root.SetKey(offset,key);
      root.SetVal(offset,value);
      
    } else {
      //insert in newleaf
      newleaf.info.numkeys += 1;
      for (offset = 0; offset < newleaf.info.numkeys; offset++){
        newleaf.GetKey(offset,testkey);
        if(testkey < key) {
          break;
        }
      }

      for (reverseoffset = newleaf.info.numkeys-1; reverseoffset >= offset && reverseoffset > 0; reverseoffset--){
        //shift keyvalue pairs over til we got the slot where we will place the key
        rc = newleaf.GetKeyVal(reverseoffset-1,kvpair);
        if (rc != ERROR_NOERROR) {return rc;}
        rc = newleaf.SetKeyVal(reverseoffset,kvpair);
        if (rc != ERROR_NOERROR) {return rc;}
      }
      newleaf.SetKey(offset,key);
      newleaf.SetVal(offset,value);
    }
    

    
    newleaf.Serialize(buffercache,newleafptr);
    root.Serialize(buffercache,ptr);
    //upsert the newleaf ptr, and the key that you are promoting

    root.GetKey(root.info.numkeys-1,promote);

    cout << "\n\n" << root << "\n--------\n" << newleaf << "\n\n";
    return Upsert(newleafptr, promote, traversednodes);
  } else {
    //leaf has room for at least
    for (offset = 0; offset < root.info.numkeys; offset++) {
      rc = root.GetKey(offset,testkey);
      if(key < testkey || key == testkey) {
        if (key == testkey) {return ERROR_CONFLICT;}
        break;
      }
    }
    //cout << "Offset is: " << offset << "\n\n";
    //now we push all the stuff from offset 1 over to the right
    root.info.numkeys += 1;
    for (reverseoffset = root.info.numkeys-1; reverseoffset >= offset && reverseoffset > 0; reverseoffset--){
      //shift keyvalue pairs over til we got the slot where we will place the key
      rc = root.GetKeyVal(reverseoffset-1,kvpair);
      if (rc != ERROR_NOERROR) {return rc;}
      rc = root.SetKeyVal(reverseoffset,kvpair);
      if (rc != ERROR_NOERROR) {return rc;}
    }
      root.SetKey(offset,key);
      root.SetVal(offset,value);
      root.Serialize(buffercache,ptr);
      cout << root << "\n\n";
      return ERROR_NOERROR;
  }

  //if the leaf is full, allocate a new node, get its disk pointer.
  //split the full leaf keys in have, putting the right hand side in the new one
  //promote the key that you have split on, and insert the key and pointer into the parent
  //by popping it off the stack.  
  return ERROR_UNIMPL;
}

ERROR_T BTreeIndex::Upsert(const SIZE_T &ptr, const KEY_T &key, std::stack<BTreeNode> traversed)
{
   BTreeNode parent;


   parent = traversed.top();
   return ERROR_UNIMPL;
}
  
ERROR_T BTreeIndex::Update(const KEY_T &key, const VALUE_T &value)
{
  // WRITE ME
  return LookupOrUpdateInternal(superblock.info.rootnode, BTREE_OP_UPDATE, (KEY_T&)key, (VALUE_T&)value);
}

  
ERROR_T BTreeIndex::Delete(const KEY_T &key)
{
  // This is optional extra credit 
  //
  // 
  return ERROR_UNIMPL;
}

  
//
//
// DEPTH first traversal
// DOT is Depth + DOT format
//

ERROR_T BTreeIndex::DisplayInternal(const SIZE_T &node,
				    ostream &o,
				    BTreeDisplayType display_type) const
{
  KEY_T testkey;
  SIZE_T ptr;
  BTreeNode b;
  ERROR_T rc;
  SIZE_T offset;

  rc= b.Unserialize(buffercache,node);

  if (rc!=ERROR_NOERROR) { 
    return rc;
  }

  rc = PrintNode(o,node,b,display_type);
  
  if (rc) { return rc; }

  if (display_type==BTREE_DEPTH_DOT) { 
    o << ";";
  }

  if (display_type!=BTREE_SORTED_KEYVAL) {
    o << endl;
  }

  switch (b.info.nodetype) { 
  case BTREE_ROOT_NODE:
  case BTREE_INTERIOR_NODE:
    if (b.info.numkeys>0) { 
      for (offset=0;offset<=b.info.numkeys;offset++) { 
	rc=b.GetPtr(offset,ptr);
	if (rc) { return rc; }
	if (display_type==BTREE_DEPTH_DOT) { 
	  o << node << " -> "<<ptr<<";\n";
	}
	rc=DisplayInternal(ptr,o,display_type);
	if (rc) { return rc; }
      }
    }
    return ERROR_NOERROR;
    break;
  case BTREE_LEAF_NODE:
    return ERROR_NOERROR;
    break;
  default:
    if (display_type==BTREE_DEPTH_DOT) { 
    } else {
      o << "Unsupported Node Type " << b.info.nodetype ;
    }
    return ERROR_INSANE;
  }

  return ERROR_NOERROR;
}


ERROR_T BTreeIndex::Display(ostream &o, BTreeDisplayType display_type) const
{
  ERROR_T rc;
  if (display_type==BTREE_DEPTH_DOT) { 
    o << "digraph tree { \n";
  }
  rc=DisplayInternal(superblock.info.rootnode,o,display_type);
  if (display_type==BTREE_DEPTH_DOT) { 
    o << "}\n";
  }
  SanityCheck();
  return ERROR_NOERROR;
}


ERROR_T BTreeIndex::SanityCheck() const
{
  // WRITE ME
  BTreeNode root;
  cout << "sanity checking" << endl;
  //invariants
  //1. every path from root to leaf is same length
  //2. if node has n children, it has n-1 keys
  //3. every node cept root is at LEAST half full
  //4. elements stored in subtree must have key values that are between keys in parent node
  //5. root has at least two children if it is not a leaf
 //Start with rootnode and check if it is empty (aka first insert)
  root.Unserialize(buffercache, superblock.info.rootnode); 
  if(root.info.keysize == 0){
     cout << "NOTHING IN THE ROOT BRUH" << endl;
  }else{
     cout << "we got someting in the root commander" << endl;
     //isBalanced(root);
  }
  return ERROR_UNIMPL;
} 


ostream & BTreeIndex::Print(ostream &os) const
{
  // WRITE ME
  return os;
}




