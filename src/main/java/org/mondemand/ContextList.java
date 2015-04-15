package org.mondemand;

import java.util.ArrayList;
import java.util.List;

/**
 * This is a container that holds a list of Contexts
 */
public class ContextList {
  protected List<Context> contextList = new ArrayList<Context>();

  public void addContext(Context context)
  {
    this.contextList.add(context);
  }

  public List<Context> getList()
  {
    return this.contextList;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result
        + ((contextList == null) ? 0 : contextList.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    ContextList other = (ContextList) obj;
    if (contextList == null) {
      if (other.contextList != null)
        return false;
    } else if (!contextList.equals(other.contextList))
      return false;
    return true;
  }

  @Override
  public String toString() {
    return "ContextList [contextList=" + contextList + "]";
  }

}
