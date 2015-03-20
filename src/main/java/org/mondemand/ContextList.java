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
}
