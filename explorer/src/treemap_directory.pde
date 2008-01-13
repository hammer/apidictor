import treemap.*;

FolderItem rootItem;
FileItem rolloverItem;
FolderItem taggedItem;

BoundsIntegrator zoomBounds;
FolderItem zoomItem;

RankedLongArray modTimes = new RankedLongArray();

PFont font;

void setRoot(File folder) {
  FolderItem tm = new FolderItem(null, folder, 0, 0);
  tm.setBounds(0, 0, width, height);
  tm.contentsVisible = true;
  
  rootItem = tm;
  rootItem.zoomIn();
  rootItem.updateColors();
}

// EVENT HANDLERS
void mousePressed() {
  if (zoomItem != null) {
    zoomItem.mousePressed();
  }
}

// SETUP
public void setup() {
  size(1024, 768);
  zoomBounds = new BoundsIntegrator(0, 0, width, height);
  
  cursor(CROSS);
  rectMode(CORNERS);
  smooth();
  noStroke();
  
  font = createFont("SansSerif", 13);
  setRoot(new File("/Applications/Processing"));
}

// DRAW
void draw() {
  background(255);
  textFont(font);
  frameRate(30);
  zoomBounds.update();
  
  rolloverItem = null;
  taggedItem = null;
  
  if (rootItem != null) {
    rootItem.draw();
  }
  if (rolloverItem != null) {
    rolloverItem.drawTitle();
  }
  if (taggedItem != null) {
    taggedItem.drawTag();
  }
}
