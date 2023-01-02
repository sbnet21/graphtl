var preset = {
	basic: {
		rule: `[
	{
		(
			match a:A-X->b:B, b-Y->c:C, c-Z->d:D, d-W->e:E 
			map {b,c}|->h:M 
			connect h-Q->e 
			disconnect h-Z->d
		)
	}, {
		(
			match m:M-Q->e:E 
			map {m,e}|->me:S
		)
	}
]`, constraint: `constraints {
	 (from:B -> edge:Y, to:C), 
	 (to:C -> edge:Y, from:B),
	 (from:M -> edge:Q, to:E),
	 (to:E -> edge:Q, from:M)}`
	, desc: "basic test"
	}
,	zoom1: {
		rule: `[
	{
		(
			match a:RESLICE-USED->b:WARP_PARAMS, b-WAS_GENERATED_BY->c:ALIGN_WARP 
			map {a,b,c}|->u:UBIO_BOX_1
		),
		(
			match a:CONVERT-USED->b:ALIAS_SLICE, b-WAS_GENERATED_BY->c:SLICER
			map {a,b,c}|->u:UBIO_BOX_2
		)
	}
]`, constraint: ""
	, desc: "Zoom query 1"
	}
}