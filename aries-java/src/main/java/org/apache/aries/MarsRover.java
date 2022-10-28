/*
 * Copyright (c) 2019 R.C
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.aries;

import org.apache.aries.common.EnumParameter;
import org.apache.aries.common.Parameter;
import org.apache.aries.common.RETURN_CODE;
import org.apache.aries.common.StringArrayParameter;
import org.apache.aries.common.ToyUtils;

import java.util.List;

public class MarsRover extends AbstractJavaToy {

  private final Parameter<String[]> commands =
      StringArrayParameter.newBuilder("mr.commands").setDescription("A series of movements for rover").setRequired().opt();
  private final Parameter<String[]> plateau_point =
      StringArrayParameter.newBuilder("mr.plateau").setDescription("The up right corner of a plateau").setRequired().setRequired().opt();
  private final Parameter<String[]> start_point =
      StringArrayParameter.newBuilder("mr.start").setDescription("Point at the plateau where rover should start").setRequired().opt();
  private final Parameter<Enum> direction =
      EnumParameter.newBuilder("mr.direction", Direction.N, Direction.class).setDescription("Face direction at start point").setRequired().opt();

  @Override
  protected void requisite(List<Parameter> requisites) {
    requisites.add(commands);
    requisites.add(plateau_point);
    requisites.add(start_point);
    requisites.add(direction);
  }

  private Plateau plateau;
  private Rover rover;

  @Override
  protected void exampleConfiguration() {
    example(commands.key(), "RLLMMMR");
    example(plateau_point.key(), "5,5");
    example(start_point.key(), "0,0");
    example(direction.key(), "E");
  }

  @Override
  protected void buildToy(ToyConfiguration configuration) throws Exception {
    ToyUtils.assertLengthValid(plateau_point.value(), 2);
    plateau = new Plateau(
        Integer.parseInt(plateau_point.value()[0]),
        Integer.parseInt(plateau_point.value()[1])
    );
    ToyUtils.assertLengthValid(start_point.value(), 2);
    Location location = new Location(
        Integer.parseInt(start_point.value()[0]),
        Integer.parseInt(start_point.value()[1])
    );
    rover = new Rover(location, (Direction)direction.value());
  }

  @Override protected int haveFun() throws Exception {
    for (String command : commands.value()) {
      LOG.info("Start from " + rover);
      rover.move(plateau, command.toCharArray());
      LOG.info("End at " + rover);
    }
    return RETURN_CODE.SUCCESS.code();
  }

  @Override protected void destroyToy() throws Exception {
  }

  @Override protected String getParameterPrefix() {
    return "mr";
  }

  static class Plateau {

    int border_x;
    int border_y;

    Plateau(int border_x, int border_y) {
      this.border_x = border_x;
      this.border_y = border_y;
    }

    boolean validate(Location loc) {
      return (0 <= loc.getX() && loc.getX() <= border_x) &&
             (0 <= loc.getY() && loc.getY() <= border_y);
    }

  }

  static class Location {

    int x;
    int y;

    Location(int x, int y) {
      this.x = x;
      this.y = y;
    }

    public int getX() {
      return x;
    }

    public int getY() {
      return y;
    }

    @Override public String toString() {
      return "{" + "x=" + x + ", y=" + y + '}';
    }

  }

  enum Direction {

    E(1, 0),
    S(0, -1),
    W(-1, 0),
    N(0, 1);

    Direction(int delta_x, int delta_y) {
      this.delta_x = delta_x;
      this.delta_y = delta_y;
    }

    private int delta_x;
    private int delta_y;

    Location moveFrom(Location loc) {
      return new Location(loc.getX() + delta_x, loc.getY() + delta_y);
    }

    Direction turnLeft() {
      if (this == E) return N;
      if (this == S) return E;
      if (this == W) return S;
                     return W;
    }

    Direction turnRight() {
      if (this == E) return S;
      if (this == S) return W;
      if (this == W) return N;
                     return E;
    }

    @Override public String toString() {
      return "{" + name() + '}';
    }

  }

  static class Rover {

    Location location;
    Direction direction;

    Rover(Location location, Direction direction) {
      this.location = location;
      this.direction = direction;
    }

    void move(Plateau plateau, char... moves) {
      for (char c : moves) {
        char uc = Character.toUpperCase(c);
        switch (uc) {
          case 'L': direction = direction.turnLeft();  break;
          case 'R': direction = direction.turnRight(); break;
          case 'M': {
            Location newLoc = direction.moveFrom(location);
            if (plateau.validate(newLoc)) {
              location = newLoc;
            }
            break;
          }
          default: break;
        }
      }
    }

    @Override public String toString() {
      return "Rover{" + "location=" + location + ", direction=" + direction + '}';
    }

  }

}
