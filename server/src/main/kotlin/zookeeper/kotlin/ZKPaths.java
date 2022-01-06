package zookeeper.kotlin;
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.Collections;
import java.util.List;

public class ZKPaths
{
    /**
     * Zookeeper's path separator character.
     */
    public static final String PATH_SEPARATOR = "/";

    private static final char PATH_SEPARATOR_CHAR = '/';

    /**
     * Apply the namespace to the given path
     *
     * @param namespace namespace (can be null)
     * @param path      path
     * @return adjusted path
     */
    public static String fixForNamespace(String namespace, String path)
    {
        return fixForNamespace(namespace, path, false);
    }

    /**
     * Apply the namespace to the given path
     *
     * @param namespace    namespace (can be null)
     * @param path         path
     * @param isSequential if the path is being created with a sequential flag
     * @return adjusted path
     */
    public static String fixForNamespace(String namespace, String path, boolean isSequential)
    {
        // Child path must be valid in and of itself.
        PathUtils.validatePath(path, isSequential);

        if ( namespace != null )
        {
            return makePath(namespace, path);
        }
        return path;
    }

    /**
     * Given a full path, return the node name. i.e. "/one/two/three" will return "three"
     *
     * @param path the path
     * @return the node
     */
    public static String getNodeFromPath(String path)
    {
        PathUtils.validatePath(path);
        int i = path.lastIndexOf(PATH_SEPARATOR_CHAR);
        if ( i < 0 )
        {
            return path;
        }
        if ( (i + 1) >= path.length() )
        {
            return "";
        }
        return path.substring(i + 1);
    }

    public static class PathAndNode
    {
        private final String path;
        private final String node;

        public PathAndNode(String path, String node)
        {
            this.path = path;
            this.node = node;
        }

        public String getPath()
        {
            return path;
        }

        public String getNode()
        {
            return node;
        }

        @Override
        public int hashCode()
        {
            final int prime = 31;
            int result = 1;
            result = prime * result + node.hashCode();
            result = prime * result + path.hashCode();
            return result;
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj)
            {
                return true;
            }
            if (obj == null)
            {
                return false;
            }
            if (getClass() != obj.getClass())
            {
                return false;
            }
            PathAndNode other = (PathAndNode) obj;
            if (!node.equals(other.node))
            {
                return false;
            }
            if (!path.equals(other.path))
            {
                return false;
            }
            return true;
        }

        @Override
        public String toString()
        {
            return "PathAndNode [path=" + path + ", node=" + node + "]";
        }
    }

    /**
     * Given a full path, return the node name and its path. i.e. "/one/two/three" will return {"/one/two", "three"}
     *
     * @param path the path
     * @return the node
     */
    public static PathAndNode getPathAndNode(String path)
    {
        PathUtils.validatePath(path);
        int i = path.lastIndexOf(PATH_SEPARATOR_CHAR);
        if ( i < 0 )
        {
            return new PathAndNode(path, "");
        }
        if ( (i + 1) >= path.length() )
        {
            return new PathAndNode(PATH_SEPARATOR, "");
        }
        String node = path.substring(i + 1);
        String parentPath = (i > 0) ? path.substring(0, i) : PATH_SEPARATOR;
        return new PathAndNode(parentPath, node);
    }

    // Hardcoded in {@link org.apache.zookeeper.server.PrepRequestProcessor}
    static final int SEQUENTIAL_SUFFIX_DIGITS = 10;

    /**
     * Extracts the ten-digit suffix from a sequential znode path. Does not currently perform validation on the
     * provided path; it will just return a string comprising the last ten characters.
     *
     * @param path the path of a sequential znodes
     * @return the sequential suffix
     */
    public static String extractSequentialSuffix(String path) {
        int length = path.length();
        return length > SEQUENTIAL_SUFFIX_DIGITS ? path.substring(length - SEQUENTIAL_SUFFIX_DIGITS) : path;
    }

    /**
     * Given a parent path and a child node, create a combined full path
     *
     * @param parent the parent
     * @param child  the child
     * @return full path
     */
    public static String makePath(String parent, String child)
    {
        // 2 is the maximum number of additional path separators inserted
        int maxPathLength = nullableStringLength(parent) + nullableStringLength(child) + 2;
        // Avoid internal StringBuilder's buffer reallocation by specifying the max path length
        StringBuilder path = new StringBuilder(maxPathLength);

        joinPath(path, parent, child);

        return path.toString();
    }

    /**
     * Given a parent path and a list of children nodes, create a combined full path
     *
     * @param parent       the parent
     * @param firstChild   the first children in the path
     * @param restChildren the rest of the children in the path
     * @return full path
     */
    public static String makePath(String parent, String firstChild, String... restChildren)
    {
        // 2 is the maximum number of additional path separators inserted
        int maxPathLength = nullableStringLength(parent) + nullableStringLength(firstChild) + 2;
        if ( restChildren != null )
        {
            for ( String child : restChildren )
            {
                // 1 is for possible additional separator
                maxPathLength += nullableStringLength(child) + 1;
            }
        }
        // Avoid internal StringBuilder's buffer reallocation by specifying the max path length
        StringBuilder path = new StringBuilder(maxPathLength);

        joinPath(path, parent, firstChild);

        if ( restChildren == null )
        {
            return path.toString();
        }
        else
        {
            for ( String child : restChildren )
            {
                joinPath(path, "", child);
            }

            return path.toString();
        }
    }

    private static int nullableStringLength(String s)
    {
        return s != null ? s.length() : 0;
    }

    /**
     * Given a parent and a child node, join them in the given {@link StringBuilder path}
     *
     * @param path   the {@link StringBuilder} used to make the path
     * @param parent the parent
     * @param child  the child
     */
    private static void joinPath(StringBuilder path, String parent, String child)
    {
        // Add parent piece, with no trailing slash.
        if ( (parent != null) && (parent.length() > 0) )
        {
            if ( parent.charAt(0) != PATH_SEPARATOR_CHAR )
            {
                path.append(PATH_SEPARATOR_CHAR);
            }
            if ( parent.charAt(parent.length() - 1) == PATH_SEPARATOR_CHAR )
            {
                path.append(parent, 0, parent.length() - 1);
            }
            else
            {
                path.append(parent);
            }
        }

        if ( (child == null) || (child.length() == 0) ||
                (child.length() == 1 && child.charAt(0) == PATH_SEPARATOR_CHAR) )
        {
            // Special case, empty parent and child
            if ( path.length() == 0 )
            {
                path.append(PATH_SEPARATOR_CHAR);
            }
            return;
        }

        // Now add the separator between parent and child.
        path.append(PATH_SEPARATOR_CHAR);

        int childAppendBeginIndex;
        if ( child.charAt(0) == PATH_SEPARATOR_CHAR )
        {
            childAppendBeginIndex = 1;
        }
        else
        {
            childAppendBeginIndex = 0;
        }

        int childAppendEndIndex;
        if ( child.charAt(child.length() - 1) == PATH_SEPARATOR_CHAR )
        {
            childAppendEndIndex = child.length() - 1;
        }
        else
        {
            childAppendEndIndex = child.length();
        }

        // Finally, add the child.
        path.append(child, childAppendBeginIndex, childAppendEndIndex);
    }

    private ZKPaths()
    {
    }
}