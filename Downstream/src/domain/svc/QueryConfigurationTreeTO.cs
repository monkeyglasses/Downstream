using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using com.bitscopic.downstream.domain.data;

namespace com.bitscopic.downstream.domain.svc
{
    /// <summary>
    /// Service based implementation of Tree specifically for QueryConfiguration
    /// </summary>
    [Serializable]
    public class QueryConfigurationTreeTO
    {
        public QueryConfigurationTreeNodeTO rootNode;

        public QueryConfigurationTreeTO() { }

        public QueryConfigurationTreeTO(Tree<QueryConfiguration> queryConfigs)
        {
            if (queryConfigs == null || queryConfigs.RootNode == null)
            {
                return;
            }

            this.rootNode = new QueryConfigurationTreeNodeTO(queryConfigs.RootNode);
        }

        public Tree<QueryConfiguration> convertToTree()
        {
            Tree<QueryConfiguration> tree = new Tree<QueryConfiguration>(this.rootNode.convertToTreeNode());

            return tree;
        }
    }

    [Serializable]
    public class QueryConfigurationTreeNodeTO
    {
        public QueryConfigurationTO value;
        public QueryConfigurationTreeNodeTO[] children;

        public QueryConfigurationTreeNodeTO() { }

        public QueryConfigurationTreeNodeTO(data.TreeNode<QueryConfiguration> treeNode)
        {
            this.value = new QueryConfigurationTO(treeNode.Value);

            if (treeNode.Children != null)
            {
                this.children = new QueryConfigurationTreeNodeTO[treeNode.Children.Count];
                for (int i = 0; i < treeNode.Children.Count; i++)
                {
                    this.children[i] = new QueryConfigurationTreeNodeTO(treeNode.Children[i]);
                }
            }
        }

        #region Object Conversions

        public TreeNode<QueryConfiguration> convertToTreeNode()
        {
            TreeNode<QueryConfiguration> root = new TreeNode<QueryConfiguration>(this.value.convertToQueryConfiguration());

            if (this.children != null && this.children.Length > 0)
            {
                for (int i = 0; i < this.children.Length; i++)
                {
                    addChildRecursive(root, this.children[i]);
                }
            }

            return root;
        }

        void addChildRecursive(TreeNode<QueryConfiguration> parent, QueryConfigurationTreeNodeTO childToTranslate)
        {
            TreeNode<QueryConfiguration> child = new TreeNode<QueryConfiguration>(childToTranslate.value.convertToQueryConfiguration());
            parent.addChild(child);
            if (childToTranslate.children != null && childToTranslate.children.Length > 0)
            {
                for (int i = 0; i < childToTranslate.children.Length; i++)
                {
                    addChildRecursive(child, childToTranslate.children[i]);
                }
            }
        }


        #endregion
    }
}
