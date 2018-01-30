import _ from "lodash";
import React from "react";

import { trustIcon } from "src/util/trust";

import collapseIcon from "!!raw-loader!assets/collapse.svg";
import expandIcon from "!!raw-loader!assets/expand.svg";

import "./expandable.styl";

const truncateLength = 50;

interface ExpandableStringProps {
  short?: string;
  long: string;
}

interface ExpandableStringState {
  expanded: boolean;
}

// ExpandableString displays a short string with a clickable ellipsis. When
// clicked, the component displays long. If short is not specified, it uses
// the first 50 chars of long.
export class ExpandableString extends React.Component<ExpandableStringProps, ExpandableStringState> {
  state: ExpandableStringState = {
    expanded: false,
  };

  onClick = (ev: any) => {
    ev.preventDefault();
    this.setState({ expanded: !this.state.expanded });
  }

  renderText(expanded: boolean) {
    if (expanded) {
      return (
        <span className="expandable__text expandable__text--expanded">{ this.props.long }</span>
      );
    }

    const short = this.props.short || this.props.long.substr(0, truncateLength).trim();
    return (
      <span className="expandable__text">{ short }&nbsp;&hellip;</span>
    );
  }

  render() {
    const { short, long } = this.props;

    const neverCollapse = _.isNil(short) && long.length <= truncateLength + 2;
    if (neverCollapse) {
      return <span>this.props.long</span>;
    }

    const { expanded } = this.state;
    const icon = expanded ? collapseIcon : expandIcon;
    return (
      <div className="expandable" onClick={ this.onClick }>
        { this.renderText(expanded) }
        <span className="expandable__icon" dangerouslySetInnerHTML={ trustIcon(icon) } />
      </div>
    );
  }
}
