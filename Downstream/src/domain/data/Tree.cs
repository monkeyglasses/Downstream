using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using com.bitscopic.downstream.domain;

namespace com.bitscopic.downstream.domain.data
{
    /// <summary>
    /// A generic Tree structure class
    /// </summary>
    /// <typeparam name="T"></typeparam>
    [Serializable]
    public class Tree<T>
    {
        TreeNode<T> _rootNode;
        /// <summary>
        /// The TreeNode at the root of the Tree structure
        /// </summary>
        public TreeNode<T> RootNode { get { return _rootNode; } }

        /// <summary>
        /// Tree constructor takes a root TreeNode as it's argument
        /// </summary>
        /// <param name="root"></param>
        public Tree(TreeNode<T> root)
        {
            if (root == null)
            {
                throw new ArgumentNullException("The root node must not be null!");
            }
            _rootNode = root;
        }

        /// <summary>
        /// Recursively search the tree for a tree node type using the objects .Equals operator
        /// </summary>
        /// <param name="node">The node value to search for</param>
        /// <returns>The node value if found, the node type's default value otherwise</returns>
        public TreeNode<T> search(T node)
        {
            if (_rootNode.Value.Equals(node))
            {
                return _rootNode;
            }
            else if (_rootNode.Children != null && _rootNode.Children.Count > 0)
            {
                foreach (TreeNode<T> currentNode in _rootNode.Children)
                {
                    Tree<T> subTree = new Tree<T>(currentNode);
                    TreeNode<T> result = subTree.search(node);
                    if (result != null)
                    {
                        return result;
                    }
                }
            }
            return null;
        }
    }

    /// <summary>
    /// A generic TreeNode class for populating the nodes of a generic Tree structure 
    /// </summary>
    /// <typeparam name="T"></typeparam>
    [Serializable]
    public class TreeNode<T>
    {
        int _depth = 0;
        public int Depth { get { return _depth; } }

        IList<TreeNode<T>> _children = new List<TreeNode<T>>();
        public IList<TreeNode<T>> Children 
        {
            get { return _children; }
            set { _children = value; } 
        }
        //TreeNode<T> _parent;
        //public TreeNode<T> Parent 
        //{
        //    get { return _parent; } 
        //    set 
        //    {
        //        _parent = value;
        //        _depth = value.Depth + 1;
        //    } 
        //}
        protected void setDepth(Int32 depth)
        {
            _depth = depth;
        }

        T _value;
        public T Value { get { return _value; } }

        /// <summary>
        /// The TreeNode constructor takes the value of the tree node
        /// </summary>
        /// <param name="value"></param>
        public TreeNode(T value)
        {
            if (value == null)
            {
                throw new ArgumentNullException("The tree node's value must not be null!");
            }
            _value = value;
        }

        public void addSelfAsChild(TreeNode<T> parent)
        {
            _depth = parent.Depth + 1;
            parent.addChild(this);
        }

        /// <summary>
        /// Add a child TreeNode to the current node's Children collection
        /// </summary>
        /// <param name="node"></param>
        public void addChild(TreeNode<T> node)
        {
            //node.Parent = this;
            _children.Add(node);
            node.setDepth(_depth + 1);
        }
    }
}
