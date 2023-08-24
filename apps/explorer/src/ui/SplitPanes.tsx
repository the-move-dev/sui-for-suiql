// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

import { ChevronLeft12, ChevronUp12 } from '@mysten/icons';
import { cva, type VariantProps } from 'class-variance-authority';
import clsx from 'clsx';
import { type ReactNode, useRef, useState } from 'react';
import {
	type Direction,
	type ImperativePanelHandle,
	Panel,
	PanelGroup,
	type PanelGroupProps,
	type PanelProps,
	PanelResizeHandle,
} from 'react-resizable-panels';

const panelResizeHandleStyles = cva(['group/container z-10'], {
	variants: {
		isHorizontal: {
			true: '',
			false: '',
		},
		isCollapsed: {
			true: '',
			false: '',
		},
		noDividerPaddings: {
			true: '',
			false: '',
		},
	},
	defaultVariants: {
		isHorizontal: false,
		isCollapsed: false,
		noDividerPaddings: false,
	},
	compoundVariants: [
		{
			isHorizontal: true,
			isCollapsed: true,
			className: 'px-6',
		},
		{
			isHorizontal: true,
			isCollapsed: false,
			className: 'px-3',
		},
		{
			isHorizontal: false,
			isCollapsed: true,
			className: 'py-6',
		},
		{
			isHorizontal: false,
			isCollapsed: false,
			className: 'py-3',
		},
		{
			isHorizontal: true,
			noDividerPaddings: true,
			isCollapsed: true,
			className: '-mx-6',
		},
		{
			isHorizontal: true,
			noDividerPaddings: true,
			isCollapsed: false,
			className: '-mx-3',
		},
		{
			isHorizontal: false,
			noDividerPaddings: true,
			isCollapsed: true,
			className: '-my-6',
		},
		{
			isHorizontal: false,
			noDividerPaddings: true,
			isCollapsed: false,
			className: '-my-3',
		},
	],
});

type PanelResizeHandleStylesProps = VariantProps<typeof panelResizeHandleStyles>;

interface ResizeHandleProps extends PanelResizeHandleStylesProps {
	togglePanelCollapse: () => void;
	collapsibleButton?: boolean;
	noHoverHidden?: boolean;
}

function ResizeHandle({
	isHorizontal,
	isCollapsed,
	collapsibleButton,
	togglePanelCollapse,
	noHoverHidden,
	noDividerPaddings,
}: ResizeHandleProps) {
	const [isDragging, setIsDragging] = useState(false);

	const ChevronButton = isHorizontal ? ChevronLeft12 : ChevronUp12;

	return (
		<PanelResizeHandle
			className={panelResizeHandleStyles({ isHorizontal, isCollapsed, noDividerPaddings })}
			onDragging={setIsDragging}
		>
			<div
				className={clsx(
					'relative bg-gray-45 group-hover/container:bg-hero',
					isHorizontal ? 'h-full w-px' : 'h-px',
					noHoverHidden && !isCollapsed && 'bg-transparent',
				)}
			>
				{collapsibleButton && (
					<button
						type="button"
						onClick={togglePanelCollapse}
						data-is-dragging={isDragging}
						className={clsx([
							'group/button',
							'flex h-6 w-6 cursor-pointer items-center justify-center rounded-full',
							'border-2 border-gray-45 bg-white text-gray-70 group-hover/container:border-hero-dark',
							'hover:bg-hero-dark hover:text-white',
							isHorizontal
								? 'absolute left-1/2 top-10 -translate-x-2/4'
								: 'absolute left-10 top-1/2 -translate-y-2/4',
							noHoverHidden && !isCollapsed && 'hidden group-hover/container:flex',
						])}
					>
						<ChevronButton
							className={clsx(
								'h-4 w-4 text-gray-45 group-hover/button:!text-white group-hover/container:text-hero-dark',
								isCollapsed && 'rotate-180',
							)}
						/>
					</button>
				)}
			</div>
		</PanelResizeHandle>
	);
}

interface SplitPanelProps extends PanelProps {
	panel: ReactNode;
	direction: Direction;
	renderResizeHandle: boolean;
	collapsibleButton?: boolean;
	noHoverHidden?: boolean;
	noDividerPaddings?: boolean;
}

function SplitPanel({
	panel,
	direction,
	renderResizeHandle,
	collapsibleButton,
	noHoverHidden,
	noDividerPaddings,
	...props
}: SplitPanelProps) {
	const ref = useRef<ImperativePanelHandle>(null);
	const [isCollapsed, setIsCollapsed] = useState(false);

	const togglePanelCollapse = () => {
		const panelRef = ref.current;

		if (panelRef) {
			if (isCollapsed) {
				panelRef.expand();
			} else {
				panelRef.collapse();
			}
		}
	};

	return (
		<>
			<Panel {...props} ref={ref} onCollapse={setIsCollapsed}>
				{panel}
			</Panel>
			{renderResizeHandle && (
				<ResizeHandle
					noDividerPaddings={noDividerPaddings}
					noHoverHidden={noHoverHidden}
					isCollapsed={isCollapsed}
					isHorizontal={direction === 'horizontal'}
					togglePanelCollapse={togglePanelCollapse}
					collapsibleButton={collapsibleButton}
				/>
			)}
		</>
	);
}

export interface SplitPanesProps extends PanelGroupProps {
	splitPanels: Omit<SplitPanelProps, 'renderResizeHandle' | 'direction'>[];
	noDividerPaddings?: boolean;
}

export function SplitPanes({ splitPanels, noDividerPaddings, ...props }: SplitPanesProps) {
	const { direction } = props;

	return (
		<PanelGroup {...props}>
			{splitPanels.map((panel, index) => (
				<SplitPanel
					className="h-full"
					key={index}
					order={index}
					renderResizeHandle={index < splitPanels.length - 1}
					direction={direction}
					noDividerPaddings={noDividerPaddings}
					{...panel}
				/>
			))}
		</PanelGroup>
	);
}
