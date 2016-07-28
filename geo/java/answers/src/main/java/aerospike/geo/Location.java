/*
 * Copyright 2016 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements WHICH ARE COMPATIBLE WITH THE APACHE LICENSE, VERSION 2.0.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package aerospike.geo;


import com.google.gson.Gson;

import java.awt.Point;
import java.lang.reflect.Type;

public class Location extends Point.Double {

  public Location(double x, double y) {
    super(x, y);
  }

  public Location(Point.Double point) { super(point.x, point.y); }

  static class GeoJSONPointDouble {
    String type = "Point";
    double[] coordinates;
  }

  public String toGeoJSONPointDouble() {
    GeoJSONPointDouble geo = new GeoJSONPointDouble();
    geo.coordinates = new double[] { x, y };
    String result = new Gson().toJson(geo);
    return result;
  }

  public static Location makeFromGeoJSONPointDouble(String geoJSON) {
    Location result = null;
    if (geoJSON != null) {
      GeoJSONPointDouble geo = new Gson().fromJson(geoJSON, (Type) GeoJSONPointDouble.class);
      result = new Location(geo.coordinates[0], geo.coordinates[1]);
    }
    return result;
  }

  @Override
  public String toString() {
    String result = String.format("[% 1.3f % 1.3f]", x, y);
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof Location) {
      Location other = (Location)obj;
      return (x == other.x) && (y == other.y);
    }
    return super.equals(obj);
  }

  public double distanceTo(Location other) {
    double xSide = other.x - x;
    double ySide = other.y - y;
    double xSide2 = xSide * xSide;
    double ySide2 = ySide * ySide;
    return Math.sqrt(xSide2 + ySide2);
  }

  public static void main(String[] args) {
    Location l1 = new Location(-.5, -.5);
    Location l2 = new Location(0.5, 0.5);
    System.out.println(l1.distanceTo(l2));
    System.out.println(l2.distanceTo(l1));
  }

  /**
   * Return a location that is partway toward a destination.
   * @param partway the distance that is partway there
   * @param destination
   * @return location partway to the destination
   */
  Location partWay(double partway, Location destination) {
    final double wholeway = distanceTo(destination);
    if (wholeway == 0) {
      return this;
    } else {
      final double portion = partway / wholeway;
      final Location result = new Location(
          this.x + portion * (destination.x - this.x),
          this.y + portion * (destination.y - this.y));
      return result;
    }
  }

}
